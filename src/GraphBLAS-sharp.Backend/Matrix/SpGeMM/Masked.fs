namespace GraphBLAS.FSharp.Backend.Matrix.SpGeMM

open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCellExtensions

module internal Masked =
    let private calculate
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        (context: ClContext)
        workGroupSize
        =

        let run =
            <@ fun (ndRange: Range1D) (leftRowPointers: ClArray<int>) (leftColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightRows: ClArray<int>) (rightColumnPointers: ClArray<int>) (rightValues: ClArray<'b>) (maskRows: ClArray<int>) (maskColumns: ClArray<int>) (resultValues: ClArray<'c>) (resultValuesBitmap: ClArray<int>) ->

                let gid = ndRange.GlobalID0
                let groupId = gid / workGroupSize
                let row = maskRows.[groupId]
                let col = maskColumns.[groupId]

                let rowBeginIdx = leftRowPointers.[row]
                let nnzPerRow = leftRowPointers.[row + 1] - rowBeginIdx

                let colBeginIdx = rightColumnPointers.[col]

                let nnzPerCol =
                    rightColumnPointers.[col + 1] - colBeginIdx

                let lid = ndRange.LocalID0

                let products = localArray<'c option> workGroupSize
                products.[lid] <- None

                let threadProcessedSize =
                    (nnzPerRow + workGroupSize - 1) / workGroupSize

                let mutable start = threadProcessedSize * lid
                let mutable i = threadProcessedSize * lid

                while i - start < threadProcessedSize && i < nnzPerRow do
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
                            let a = leftValues.[rowBeginIdx + i]
                            let b = rightValues.[middle]
                            let increase = (%opMul) a b

                            let product = products.[lid]

                            match product, increase with
                            | Some x, Some y ->
                                let buff = (%opAdd) x y
                                products.[lid] <- buff
                            | None, Some _ -> products.[lid] <- increase
                            | _ -> ()

                            // Break alternative
                            leftEdge <- rightEdge

                    i <- i + 1

                // Sum up all products
                let mutable step = 2

                while step <= workGroupSize do
                    barrierLocal ()

                    if lid < workGroupSize / step then
                        let i = step * (lid + 1) - 1

                        let increase = products.[i - (step >>> 1)]

                        match increase, products.[i] with
                        | Some x, Some y ->
                            let buff = (%opAdd) x y
                            products.[i] <- buff
                        | Some _, None -> products.[i] <- increase
                        | _ -> ()

                    step <- step <<< 1

                if lid = 0 then
                    match products.[workGroupSize - 1] with
                    | Some p ->
                        resultValues.[groupId] <- p
                        resultValuesBitmap.[groupId] <- 1
                    | None -> resultValuesBitmap.[groupId] <- 0 @>

        let program = context.Compile(run)

        fun (queue: MailboxProcessor<_>) (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSC<'b>) (mask: ClMatrix.COO<_>) ->

            let values =
                context.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, mask.NNZ)

            let bitmap =
                context.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, mask.NNZ)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(workGroupSize * mask.NNZ, workGroupSize)

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
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
                            bitmap)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(kernel))

            values, bitmap

    let run
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        (context: ClContext)
        workGroupSize
        =

        let calculate =
            calculate opAdd opMul context workGroupSize

        let scatter =
            Scatter.lastOccurrence context workGroupSize

        let scatterData =
            Scatter.lastOccurrence context workGroupSize

        let scanInPlace =
            PrefixSum.standardExcludeInPlace context workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSC<'b>) (mask: ClMatrix.COO<_>) ->

            let values, positions =
                calculate queue matrixLeft matrixRight mask

            let resultNNZ =
                (scanInPlace queue positions).ToHostAndFree(queue)

            let resultRows = context.CreateClArray<int> resultNNZ
            let resultColumns = context.CreateClArray<int> resultNNZ
            let resultValues = context.CreateClArray<'c> resultNNZ

            scatter queue positions mask.Rows resultRows
            scatter queue positions mask.Columns resultColumns
            scatterData queue positions values resultValues

            queue.Post(Msg.CreateFreeMsg<_>(values))
            queue.Post(Msg.CreateFreeMsg<_>(positions))

            { Context = context
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixRight.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }
