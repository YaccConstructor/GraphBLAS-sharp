namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix.CSR.Elementwise
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

        fun (processor: MailboxProcessor<_>) flag (rowPointers: ClArray<int>) nnz rowCount ->

            let rows = create processor flag nnz 0

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

        fun (processor: MailboxProcessor<_>) flag (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                prepare processor flag matrix.RowPointers matrix.Columns.Length matrix.RowCount

            let cols = copy processor flag matrix.Columns
            let vals = copyData processor flag matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = cols
              Values = vals }

    let toCOOInplace (clContext: ClContext) workGroupSize =
        let prepare =
            expandRowPointers clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                prepare processor flag matrix.RowPointers matrix.Columns.Length matrix.RowCount

            processor.Post(Msg.CreateFreeMsg(matrix.RowPointers))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = matrix.Columns
              Values = matrix.Values }

    ///<remarks>Old version</remarks>
    let elementwiseWithCOO (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =

        let prepareRows =
            expandRowPointers clContext workGroupSize

        let eWiseCOO =
            COOMatrix.elementwise clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (m1: ClMatrix.CSR<'a>) (m2: ClMatrix.CSR<'b>) ->
            let m1COO =
                { Context = clContext
                  RowCount = m1.RowCount
                  ColumnCount = m1.ColumnCount
                  Rows = prepareRows processor flag m1.RowPointers m1.Values.Length m1.RowCount
                  Columns = m1.Columns
                  Values = m1.Values }

            let m2COO =
                { Context = clContext
                  RowCount = m2.RowCount
                  ColumnCount = m2.ColumnCount
                  Rows = prepareRows processor flag m2.RowPointers m2.Values.Length m2.RowCount
                  Columns = m2.Columns
                  Values = m2.Values }

            let m3COO = eWiseCOO processor flag m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            toCSRInplace processor flag m3COO

    ///<remarks>Old version</remarks>
    let elementwiseAtLeastOneWithCOO
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        elementwiseWithCOO clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let transposeInplace (clContext: ClContext) workGroupSize =

        let toCOOInplace = toCOOInplace clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) flag (matrix: ClMatrix.CSR<'a>) ->
            toCOOInplace queue flag matrix
            |> transposeInplace queue
            |> toCSRInplace queue flag

    let transpose (clContext: ClContext) workGroupSize =

        let toCOO = toCOO clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) flag (matrix: ClMatrix.CSR<'a>) ->
            toCOO queue flag matrix
            |> transposeInplace queue
            |> toCSRInplace queue flag

    let elementwiseToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd workGroupSize

        let setPositions =
            Matrix.Common.setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) flag (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->

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
                setPositions queue flag allRows allColumns allValues positions

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

    let elementwise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let elementwiseToCOO =
            elementwiseToCOO clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) flag (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            elementwiseToCOO queue flag matrixLeft matrixRight
            |> toCSRInplace queue flag

    let elementwiseAtLeastOneToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        elementwiseToCOO clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let elementwiseAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        elementwise clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let spgemmCSC
        (clContext: ClContext)
        workGroupSize
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        =

        let run =
            SpGEMM.run clContext workGroupSize opAdd opMul

        fun (queue: MailboxProcessor<_>) (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSC<'b>) (mask: ClMask2D) ->

            run queue matrixLeft matrixRight mask
