namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix.CSR.Map2
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module CSRMatrix =
    let private expandRowPointers (clContext: ClContext) workGroupSize =

        let expandRowPointers =
            <@ fun (ndRange: Range1D) (rowPointers: ClArray<int>) (rowCount: int) (rows: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < rowCount then
                    let rowPointer = rowPointers.[i]

                    if rowPointer <> rowPointers.[i + 1] then
                        rows.[rowPointer] <- i @>

        let program = clContext.Compile(expandRowPointers)

        let create = ClArray.create clContext workGroupSize

        let scan =
            ClArray.prefixSumIncludeInplace <@ max @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rowPointers: ClArray<int>) nnz rowCount ->

            let rows = create processor allocationMode nnz 0

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(rowCount, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowPointers rowCount rows))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            let total = clContext.CreateClCell()
            ignore <| scan processor rows total 0
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

            let vals =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = cols
              Values = vals }

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

    ///<remarks>Old version</remarks>
    let map2WithCOO (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =

        let prepareRows =
            expandRowPointers clContext workGroupSize

        let eWiseCOO =
            COOMatrix.map2 clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (m1: ClMatrix.CSR<'a>) (m2: ClMatrix.CSR<'b>) ->
            let m1COO =
                { Context = clContext
                  RowCount = m1.RowCount
                  ColumnCount = m1.ColumnCount
                  Rows = prepareRows processor allocationMode m1.RowPointers m1.Values.Length m1.RowCount
                  Columns = m1.Columns
                  Values = m1.Values }

            let m2COO =
                { Context = clContext
                  RowCount = m2.RowCount
                  ColumnCount = m2.ColumnCount
                  Rows = prepareRows processor allocationMode m2.RowPointers m2.Values.Length m2.RowCount
                  Columns = m2.Columns
                  Values = m2.Values }

            let m3COO =
                eWiseCOO processor allocationMode m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            toCSRInplace processor allocationMode m3COO

    ///<remarks>Old version</remarks>
    let map2AtLeastOneWithCOO (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =

        map2WithCOO clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let transposeInplace (clContext: ClContext) workGroupSize =

        let toCOOInplace = toCOOInplace clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOOInplace queue allocationMode matrix
            |> transposeInplace queue
            |> toCSRInplace queue allocationMode

    let transpose (clContext: ClContext) workGroupSize =

        let toCOO = toCOO clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOO queue allocationMode matrix
            |> transposeInplace queue
            |> toCSRInplace queue allocationMode

    let map2ToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd workGroupSize

        let setPositions =
            Matrix.Common.setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->

            let allRows, allColumns, leftMergedValues, rightMergedValues, isRowEnd, isLeft =
                merge
                    queue
                    matrixLeft.RowPointers
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.RowPointers
                    matrixRight.Columns
                    matrixRight.Values

            let positions, allValues =
                preparePositions queue allColumns leftMergedValues rightMergedValues isRowEnd isLeft

            queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

            let resultRows, resultColumns, resultValues, _ =
                setPositions queue allocationMode allRows allColumns allValues positions

            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
            queue.Post(Msg.CreateFreeMsg<_>(positions))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }

    let map2<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let elementwiseToCOO = map2ToCOO clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            elementwiseToCOO queue allocationMode matrixLeft matrixRight
            |> toCSRInplace queue allocationMode

    let map2AtLeastOneToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        map2ToCOO clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let map2AtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        map2 clContext (Convert.atLeastOneToOption opAdd) workGroupSize

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
