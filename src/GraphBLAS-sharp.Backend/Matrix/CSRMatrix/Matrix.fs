namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext

module Matrix =
    let private expandRowPointers (clContext: ClContext) workGroupSize =

        let expandRowPointers =
            <@ fun (ndRange: Range1D) (rowPointers: ClArray<int>) (rowCount: int) (rows: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < rowCount then
                    let rowPointer = rowPointers.[i]

                    if rowPointer <> rowPointers.[i + 1] then
                        rows.[rowPointer] <- i @>

        let program = clContext.Compile(expandRowPointers)

        let create =
            ClArray.zeroCreate clContext workGroupSize

        let scan =
            PrefixSum.runIncludeInplace <@ max @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rowPointers: ClArray<int>) nnz rowCount ->

            let rows = create processor allocationMode nnz

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(rowCount, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowPointers rowCount rows))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            let total = scan processor rows 0
            processor.Post(Msg.CreateFreeMsg(total))

            rows

    let toCOO (clContext: ClContext) workGroupSize =
        let prepare =
            expandRowPointers clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                prepare processor allocationMode matrix.RowPointers matrix.Columns.Length matrix.RowCount

            let cols =
                copy processor allocationMode matrix.Columns

            let values =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = cols
              Values = values }

    let toCOOInplace (clContext: ClContext) workGroupSize =
        let prepare =
            expandRowPointers clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                prepare processor allocationMode matrix.RowPointers matrix.Columns.Length matrix.RowCount

            processor.Post(Msg.CreateFreeMsg(matrix.RowPointers))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = matrix.Columns
              Values = matrix.Values }

    let map = CSR.Map.run

    let map2 = Map2.run

    let map2AtLeastOneToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2AtLeastOne.runToCOO clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let map2AtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2AtLeastOne.run clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let transposeInplace (clContext: ClContext) workGroupSize =

        let toCOOInplace = toCOOInplace clContext workGroupSize

        let transposeInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COO.Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOOInplace queue allocationMode matrix
            |> transposeInplace queue
            |> toCSRInplace queue allocationMode

    let transpose (clContext: ClContext) workGroupSize =

        let toCOO = toCOO clContext workGroupSize

        let transposeInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COO.Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOO queue allocationMode matrix
            |> transposeInplace queue
            |> toCSRInplace queue allocationMode

    let spgemmCSC
        (clContext: ClContext)
        workGroupSize
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        =

        let run =
            SpGEMM.run clContext workGroupSize opAdd opMul

        fun (queue: MailboxProcessor<_>) (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSC<'b>) (mask: ClMatrix.COO<_>) ->

            run queue matrixLeft matrixRight mask
