namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend

module internal SpGEMMSimple =
    let private runNonEmpty
        (times: Expr<'a -> 'a -> 'a>)
        (plus: Expr<'a -> 'a -> 'a>)
        (zero: 'a)
        (clContext: ClContext)
        workGroupSize =

        let prepareMatrix = PrepareMatrix.run clContext workGroupSize
        let setup = Setup.run clContext workGroupSize
        let expand = Expansion.run times clContext workGroupSize
        let sort = Sorting.run clContext workGroupSize
        let compress = Compression.run plus clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let preparedMatrixLeft = prepareMatrix processor matrixLeft matrixRight

            let initialResult, headFlags = setup processor preparedMatrixLeft matrixRight

            let expandedResult = expand processor headFlags preparedMatrixLeft matrixRight initialResult
            processor.Post(Msg.CreateFreeMsg<_>(preparedMatrixLeft.RowPointers))
            processor.Post(Msg.CreateFreeMsg<_>(preparedMatrixLeft.Columns))
            processor.Post(Msg.CreateFreeMsg<_>(preparedMatrixLeft.Values))
            processor.Post(Msg.CreateFreeMsg<_>(initialResult.Columns))
            processor.Post(Msg.CreateFreeMsg<_>(initialResult.Values))

            let sortedResult, resultHeadFlags = sort processor expandedResult
            // processor.Post(Msg.CreateFreeMsg<_>(expandedResult.Columns))

            let result = compress processor sortedResult resultHeadFlags zero
            processor.Post(Msg.CreateFreeMsg<_>(sortedResult.Columns))
            processor.Post(Msg.CreateFreeMsg<_>(sortedResult.Values))

            result

    let run
        (times: Expr<'a -> 'a -> 'a>)
        (plus: Expr<'a -> 'a -> 'a>)
        (zero: 'a)
        (clContext: ClContext)
        workGroupSize =

        let runNonEmpty = runNonEmpty times plus zero clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            if matrixLeft.ColumnCount <> matrixRight.RowCount then
                invalidArg "matrixRight" "Column count of the left matrix must be equal to row count of the right one"

            if matrixLeft.Values.Length = 0 || matrixRight.Values.Length = 0 then
                {
                    Context = clContext
                    RowCount = matrixLeft.RowCount
                    ColumnCount = matrixRight.ColumnCount
                    RowPointers = clContext.CreateClArray([|0|])
                    Columns = clContext.CreateClArray([||])
                    Values = clContext.CreateClArray([||])
                }
            else
                runNonEmpty processor matrixLeft matrixRight
