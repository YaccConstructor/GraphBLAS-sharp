namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend

// module internal rec SpGEMMSimple =
module rec SpGEMMSimple =
    let run (clContext: ClContext) workGroupSize =

        fun (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) (times: Expr<'a -> 'a -> 'a>) (plus: Expr<'a -> 'a -> 'a>) (zero: 'a) ->
            if matrixLeft.ColumnCount <> matrixRight.RowCount then
                invalidArg "matrixRight" "Column count of the left matrix must be equal to row count of the right one"

            if matrixLeft.Values.Length = 0 || matrixRight.Values.Length = 0 then
                {
                    RowCount = matrixLeft.RowCount
                    ColumnCount = matrixRight.ColumnCount
                    RowPointers = clContext.CreateClArray(0)
                    Columns = clContext.CreateClArray(0)
                    Values = clContext.CreateClArray(0)
                }
            else
                runNonEmpty clContext workGroupSize processor matrixLeft matrixRight times plus zero

    let private runNonEmpty (clContext: ClContext) workGroupSize (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) (times: Expr<'a -> 'a -> 'a>) (plus: Expr<'a -> 'a -> 'a>) (zero: 'a) =
        let preparedMatrixLeft = PrepareMatrix.run clContext workGroupSize processor matrixLeft matrixRight
        let initialResult = Setup.run clContext workGroupSize processor preparedMatrixLeft matrixRight
        let expandedResult = Expansion.run clContext workGroupSize processor preparedMatrixLeft matrixRight initialResult times
        let sortedResult = Sorting.run clContext workGroupSize processor expandedResult

        Compression.run clContext workGroupSize processor sortedResult plus zero
