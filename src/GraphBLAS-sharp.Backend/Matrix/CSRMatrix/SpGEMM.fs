namespace GraphBLAS.FSharp.Backend

open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Predefined

module internal SpGEMM =
    let run
        (context: ClContext)
        workGroupSize
        (opAdd: Expr<'c option -> 'c option -> 'c option>)
        (opMul: Expr<'a option -> 'b option -> 'c option>)
        =

        let scatter = Scatter.runInplace context workGroupSize
        let scatterData = Scatter.runInplace context workGroupSize
        let scanInplace = PrefixSum.standardExcludeInplace context workGroupSize

        let run =
            <@
                fun (ndRange: Range1D)
                    (leftRowPointers: ClArray<int>)
                    (leftColumns: ClArray<int>)
                    (leftValues: ClArray<'a>)
                    (rightRows: ClArray<int>)
                    (rightColumnPointers: ClArray<int>)
                    (rightValues: ClArray<'b>)
                    (maskRows: ClArray<int>)
                    (maskColumns: ClArray<int>)
                    (resultValues: ClArray<'c>)
                    (resultValuesBitmap: ClArray<int>)
                    ->

                    let gid = ndRange.GlobalID0
                    let groupId = gid / workGroupSize
                    let row = maskRows.[groupId]
                    let col = maskColumns.[groupId]

                    let rowBeginIdx = leftRowPointers.[row]
                    let nnzPerRow = leftRowPointers.[row + 1] - rowBeginIdx

                    let colBeginIdx = rightColumnPointers.[col]
                    let nnzPerCol = rightColumnPointers.[col + 1] - colBeginIdx

                    // let mutable arrayToProcess = rightRows
                    // let mutable valuesToProcess = rightValues
                    // let mutable processedSize = nnzPerCol
                    // let mutable arrayToSearch = leftColumns
                    // let mutable valuesToSearch = leftValues
                    // let mutable searchSize = nnzPerRow
                    // if nnzPerRow < nnzPerCol then
                    //     arrayToProcess <- leftColumns
                    //     valuesToProcess <- leftValues
                    //     processedSize <- nnzPerRow
                    //     arrayToSearch <- rightRows
                    //     valuesToSearch <- rightValues
                    //     searchSize <- nnzPerCol

                    let lid = ndRange.LocalID0

                    let products = localArray<'c option> workGroupSize
                    products.[lid] <- None

                    barrierLocal()

                    let mutable i = lid
                    while i < nnzPerRow do
                        let indexToFind = leftColumns.[rowBeginIdx + i]

                        // Binary search
                        let mutable leftEdge = colBeginIdx
                        let mutable rightEdge = leftEdge + nnzPerCol
                        while leftEdge < rightEdge do
                            let middle = (leftEdge + rightEdge) / 2
                            let foundIdx = rightRows.[middle]

                            if foundIdx < indexToFind then
                                leftEdge <- middle + 1
                            elif foundIdx > indexToFind then
                                rightEdge <- middle
                            else
                                // Found needed index
                                let product = (%opMul) (Some leftValues.[rowBeginIdx + i]) (Some rightValues.[middle])
                                products.[lid] <- (%opAdd) products.[lid] product

                        i <- i + workGroupSize

                    // Sum up all products
                    let mutable step = 2
                    while step <= workGroupSize do
                        barrierLocal ()

                        if lid < workGroupSize / step then
                            let i = step * (lid + 1) - 1
                            products.[i] <- (%opAdd) products.[i - (step >>> 1)] products.[i]

                        step <- step <<< 1

                    barrierLocal ()

                    if lid = workGroupSize - 1 then
                        match products.[lid] with
                        | Some p ->
                            resultValues.[groupId] <- p
                            resultValuesBitmap.[groupId] <- 1
                        | None ->
                            resultValuesBitmap.[groupId] <- 0
            @>
        let program = context.Compile(run)

        fun (queue: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSCMatrix<'b>)
            (mask: Mask2D) ->

            let values = context.CreateClArray<'c> mask.Rows.Length
            let bitmap = context.CreateClArray<int> mask.Rows.Length

            let kernel = program.GetKernel()

            let ndRange = Range1D(workGroupSize * mask.Rows.Length)
            queue.Post(Msg.MsgSetArguments(fun () ->
                kernel.KernelFunc
                    ndRange
                    matrixLeft.RowPointers
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.Rows
                    matrixRight.ColumnPointers
                    matrixRight.Values
                    mask.Rows
                    mask.Columns
                    values
                    bitmap))
            queue.Post(Msg.CreateRunMsg<_, _>(kernel))

            let total = context.CreateClCell()
            let positions, total = scanInplace queue bitmap total

            let resultNNZ =
                let res = [| 0 |]
                let res = queue.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(total, res, ch))
                queue.Post(Msg.CreateFreeMsg<_>(total))
                res.[0]

            let resultRows = context.CreateClArray<int> resultNNZ
            let resultCols = context.CreateClArray<int> resultNNZ
            let resultVals = context.CreateClArray<'c> resultNNZ

            scatter queue positions mask.Rows resultRows
            scatter queue positions mask.Columns resultCols
            scatterData queue positions values resultVals

            queue.Post(Msg.CreateFreeMsg<_>(values))
            queue.Post(Msg.CreateFreeMsg<_>(positions))

            { Context = context
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixRight.ColumnCount
              Rows = resultRows
              Columns = resultCols
              Values = resultVals }
