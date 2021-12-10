namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal Sorting =
    let run
        (clContext: ClContext)
        workGroupSize =

        //необязательно переводить в COO, достаточно знать headFlags для rowPointers
        let toCOO = Converter.toCOO clContext workGroupSize
        let pack = ClArray.pack clContext workGroupSize
        let unpack = ClArray.unpack clContext workGroupSize
        // TODO: возможность выбрать число бит
        let sortByKeyInPlace = RadixSort.sortByKeyInPlace8 clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->
                let matrixCOO = toCOO processor matrix

                let packedIndices = pack processor matrixCOO.Rows matrixCOO.Columns

                sortByKeyInPlace processor packedIndices matrix.Values

                let sortedRows, sortedColumns = unpack processor packedIndices

                processor.Post(Msg.CreateFreeMsg<_>(packedIndices))
                processor.Post(Msg.CreateFreeMsg<_>(sortedRows))

                {
                    RowCount = matrix.RowCount
                    ColumnCount = matrix.ColumnCount
                    RowPointers = matrix.RowPointers
                    Columns = sortedColumns
                    Values = matrix.Values
                }
