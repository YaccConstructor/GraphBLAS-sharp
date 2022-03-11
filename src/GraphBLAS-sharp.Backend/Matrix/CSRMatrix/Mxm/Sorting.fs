namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Common

module internal Sorting =
    let run
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create 0 clContext workGroupSize
        let scatter = Scatter.constInPlace 1 clContext workGroupSize
        let scan = PrefixSum.standardInclude clContext workGroupSize
        let pack = ClArray.pack clContext workGroupSize
        let unpack = ClArray.unpack clContext workGroupSize
        // TODO: возможность выбрать число бит
        let sortByKeyInPlace = RadixSort.sortByKeyInPlace8 clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->
                let headFlags = create processor matrix.Columns.Length
                scatter processor matrix.RowPointers headFlags

                let sum = clContext.CreateClCell()
                let keys, _ = scan processor headFlags sum
                processor.Post(Msg.CreateFreeMsg<_>(sum))

                let packedIndices = pack processor keys matrix.Columns
                processor.Post(Msg.CreateFreeMsg<_>(keys))

                sortByKeyInPlace processor packedIndices matrix.Values

                let sortedRows, sortedColumns = unpack processor packedIndices

                processor.Post(Msg.CreateFreeMsg<_>(packedIndices))
                processor.Post(Msg.CreateFreeMsg<_>(sortedRows))

                {
                    Context = clContext
                    RowCount = matrix.RowCount
                    ColumnCount = matrix.ColumnCount
                    RowPointers = matrix.RowPointers
                    Columns = sortedColumns
                    Values = matrix.Values
                },
                headFlags
