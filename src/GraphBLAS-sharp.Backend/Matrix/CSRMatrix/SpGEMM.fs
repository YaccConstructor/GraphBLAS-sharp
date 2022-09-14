namespace GraphBLAS.FSharp.Backend

open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Predefined

module internal SpGEMM =
    let private calculate
        (context: ClContext)
        workGroupSize
        (opAdd: Expr<'c option -> 'c option -> 'c option>)
        (opMul: Expr<'a option -> 'b option -> 'c option>)
        =

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
                                let buff = (%opAdd) products.[lid] product
                                products.[lid] <- buff

                                // Break alternative
                                leftEdge <- rightEdge

                        i <- i + workGroupSize

                    // Sum up all products
                    let mutable step = 2
                    while step <= workGroupSize do
                        barrierLocal()

                        if lid < workGroupSize / step then
                            let i = step * (lid + 1) - 1
                            let buff = (%opAdd) products.[i - (step >>> 1)] products.[i]
                            products.[i] <- buff

                        step <- step <<< 1

                    barrierLocal()

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
            (mask: Mask2D)
            (values: ClArray<'c>)
            (bitmap: ClArray<int>) ->

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(workGroupSize * mask.NNZ, workGroupSize)
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

            values, bitmap

    let run
        (context: ClContext)
        workGroupSize
        (opAdd: Expr<'c option -> 'c option -> 'c option>)
        (opMul: Expr<'a option -> 'b option -> 'c option>)
        =

        let calculate = calculate context workGroupSize opAdd opMul

        let scatter = Scatter.runInplace context workGroupSize
        let scatterData = Scatter.runInplace context workGroupSize
        let scanInplace = PrefixSum.standardExcludeInplace context workGroupSize

        fun (queue: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSCMatrix<'b>)
            (mask: Mask2D) ->

            let values = context.CreateClArray<'c>(
                    mask.NNZ,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )
            let bitmap = context.CreateClArray<int>(
                    mask.NNZ,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let values, bitmap = calculate queue matrixLeft matrixRight mask values bitmap

            let total = context.CreateClCell 0
            let positions, total = scanInplace queue bitmap total

            let resultNNZ =
                let res =
                    queue.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(total, Array.zeroCreate 1, ch))
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
