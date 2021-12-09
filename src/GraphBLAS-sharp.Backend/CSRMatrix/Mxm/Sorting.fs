namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal Sorting =
    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrix: CSRMatrix<'a>) =
            //необязательно переводить в COO, достаточно знать headFlags для rowPointers
            let matrixCOO = Converter.toCOO clContext workGroupSize processor matrix

            let packedIndices = ClArray.pack clContext workGroupSize processor matrixCOO.Rows matrixCOO.Columns

            RadixSort.sortByKeyInPlace8 clContext workGroupSize processor packedIndices matrix.Values

            let sortedRows, sortedColumns = ClArray.unpack clContext workGroupSize processor packedIndices

            processor.Post(Msg.CreateFreeMsg<_>(packedIndices))
            processor.Post(Msg.CreateFreeMsg<_>(sortedRows))

            { RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = matrix.RowPointers
              Columns = sortedColumns
              Values = matrix.Values }
