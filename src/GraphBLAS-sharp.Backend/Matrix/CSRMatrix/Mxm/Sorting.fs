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
        let sort = RadixSort.runInplace clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->
                let headFlags = create processor matrix.Columns.Length
                scatter processor matrix.RowPointers headFlags

                let headFlags' = copy processor headFlags

                let sortedColumns, sortedValues = sort processor matrix.Columns matrix.Values headFlags' (matrix.ColumnCount - 1)

                processor.Post(Msg.CreateFreeMsg<_>(headFlags'))

                {
                    Context = clContext
                    RowCount = matrix.RowCount
                    ColumnCount = matrix.ColumnCount
                    RowPointers = matrix.RowPointers
                    Columns = sortedColumns
                    Values = sortedValues
                },
                headFlags
