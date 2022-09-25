namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Elementwise
open Microsoft.FSharp.Quotations

module CSRMatrix =
    let private prepareRows (clContext: ClContext) workGroupSize =

        let prepareRows =
            <@ fun (ndRange: Range1D) (rowPointers: ClArray<int>) (rowCount: int) (rows: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < rowCount then
                    let rowPointer = rowPointers.[i]

                    if rowPointer <> rowPointers.[i + 1] then
                        rows.[rowPointer] <- i @>

        let program = clContext.Compile(prepareRows)

        let create = ClArray.create clContext workGroupSize

        let scan =
            PrefixSum.runIncludeInplace <@ max @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (rowPointers: ClArray<int>) nnz rowCount ->

            let rows = create processor nnz 0

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(rowCount, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowPointers rowCount rows))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            let total = clContext.CreateClCell()
            let _ = scan processor rows total 0
            processor.Post(Msg.CreateFreeMsg(total))

            rows

    let toCOO (clContext: ClContext) workGroupSize =
        let prepare = prepareRows clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let rows =
                prepare processor matrix.RowPointers matrix.Columns.Length matrix.RowCount

            let cols = copy processor matrix.Columns
            let vals = copyData processor matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = cols
              Values = vals }

    let toCOOInplace (clContext: ClContext) workGroupSize =
        let prepare = prepareRows clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let rows =
                prepare processor matrix.RowPointers matrix.Columns.Length matrix.RowCount

            processor.Post(Msg.CreateFreeMsg(matrix.RowPointers))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = matrix.Columns
              Values = matrix.Values }

    ///<remarks>Old version</remarks>
    let elementwiseWithCOO (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =

        let prepareRows = prepareRows clContext workGroupSize

        let eWiseCOO =
            COOMatrix.elementwise clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'b>) ->
            let m1COO =
                { Context = clContext
                  RowCount = m1.RowCount
                  ColumnCount = m1.ColumnCount
                  Rows = prepareRows processor m1.RowPointers m1.Values.Length m1.RowCount
                  Columns = m1.Columns
                  Values = m1.Values }

            let m2COO =
                { Context = clContext
                  RowCount = m2.RowCount
                  ColumnCount = m2.ColumnCount
                  Rows = prepareRows processor m2.RowPointers m2.Values.Length m2.RowCount
                  Columns = m2.Columns
                  Values = m2.Values }

            let m3COO = eWiseCOO processor m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            toCSRInplace processor m3COO

    ///<remarks>Old version</remarks>
    let elementwiseAtLeastOneWithCOO
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        let prepareRows = prepareRows clContext workGroupSize

        let eWiseCOO =
            COOMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'b>) ->
            let m1COO =
                { Context = clContext
                  RowCount = m1.RowCount
                  ColumnCount = m1.ColumnCount
                  Rows = prepareRows processor m1.RowPointers m1.Values.Length m1.RowCount
                  Columns = m1.Columns
                  Values = m1.Values }

            let m2COO =
                { Context = clContext
                  RowCount = m2.RowCount
                  ColumnCount = m2.ColumnCount
                  Rows = prepareRows processor m2.RowPointers m2.Values.Length m2.RowCount
                  Columns = m2.Columns
                  Values = m2.Values }

            let m3COO = eWiseCOO processor m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            toCSRInplace processor m3COO

    let transposeInplace (clContext: ClContext) workGroupSize =

        let toCOOInplace = toCOOInplace clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let coo = toCOOInplace queue matrix
            let transposedCoo = transposeInplace queue coo
            toCSRInplace queue transposedCoo


    let transpose (clContext: ClContext) workGroupSize =

        let toCOO = toCOO clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let coo = toCOO queue matrix
            let transposedCoo = transposeInplace queue coo
            toCSRInplace queue transposedCoo

    let elementwise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd Utils.defaultWorkGroupSize

        let setPositions =
            setPositions<'c> clContext Utils.defaultWorkGroupSize

        let prepareRowPointers =
            COOMatrix.prepareRowPointers clContext Utils.defaultWorkGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'b>) ->

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

            let resultRows, resultColumns, resultValues, positions, positionsSum =
                setPositions queue allRows allColumns allValues positions

            let rowPointers =
                prepareRowPointers queue resultRows matrixLeft.RowCount

            queue.Post(Msg.CreateFreeMsg<_>(resultRows))
            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
            queue.Post(Msg.CreateFreeMsg<_>(positions))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              RowPointers = rowPointers
              Columns = resultColumns
              Values = resultValues }

    let elementwiseAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositionsAtLeastOne clContext opAdd Utils.defaultWorkGroupSize

        let setPositions =
            setPositions<'c> clContext Utils.defaultWorkGroupSize

        let prepareRowPointers =
            COOMatrix.prepareRowPointers clContext Utils.defaultWorkGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'b>) ->

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

            let resultRows, resultColumns, resultValues, positions, positionsSum =
                setPositions queue allRows allColumns allValues positions

            let rowPointers =
                prepareRowPointers queue resultRows matrixLeft.RowCount

            queue.Post(Msg.CreateFreeMsg<_>(resultRows))
            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
            queue.Post(Msg.CreateFreeMsg<_>(positions))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              RowPointers = rowPointers
              Columns = resultColumns
              Values = resultValues }
