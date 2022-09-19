namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
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
    let elementwise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =

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
    let elementwiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =

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
