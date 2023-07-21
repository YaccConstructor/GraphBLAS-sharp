namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open Matrix
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClVectorExtensions

module MatrixExtensions =
    // Matrix.Free
    type ClMatrix.COO<'a when 'a: struct> with
        member this.Free(q: MailboxProcessor<_>) =
            this.Columns.Free q
            this.Values.Free q
            this.Rows.Free q

        member this.ToHost(q: MailboxProcessor<_>) =
            { RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows = this.Rows.ToHost q
              Columns = this.Columns.ToHost q
              Values = this.Values.ToHost q }

        member this.ToHostAndFree(q: MailboxProcessor<_>) =
            let result = this.ToHost q
            this.Free q

            result

    type ClMatrix.CSR<'a when 'a: struct> with
        member this.Free(q: MailboxProcessor<_>) =
            this.Values.Free q
            this.Columns.Free q
            this.RowPointers.Free q

        member this.ToHost(q: MailboxProcessor<_>) =
            { RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              RowPointers = this.RowPointers.ToHost q
              ColumnIndices = this.Columns.ToHost q
              Values = this.Values.ToHost q }

        member this.ToHostAndFree(q: MailboxProcessor<_>) =
            let result = this.ToHost q
            this.Free q

            result

    type ClMatrix.CSC<'a when 'a: struct> with
        member this.Free(q: MailboxProcessor<_>) =
            this.Values.Free q
            this.Rows.Free q
            this.ColumnPointers.Free q

        member this.ToHost(q: MailboxProcessor<_>) =
            { RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              RowIndices = this.Rows.ToHost q
              ColumnPointers = this.ColumnPointers.ToHost q
              Values = this.Values.ToHost q }

        member this.ToHostAndFree(q: MailboxProcessor<_>) =
            let result = this.ToHost q
            this.Free q

            result

    type ClMatrix.LIL<'a when 'a: struct> with
        member this.Free(q: MailboxProcessor<_>) =
            this.Rows
            |> List.iter (Option.iter (fun row -> row.Dispose q))

        member this.ToHost(q: MailboxProcessor<_>) =
            { RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows =
                  this.Rows
                  |> List.map (Option.map (fun row -> row.ToHost q))
              NNZ = this.NNZ }

        member this.ToHostAndFree(q: MailboxProcessor<_>) =
            let result = this.ToHost q
            this.Free q

            result

    type ClMatrix<'a when 'a: struct> with
        member this.ToHost(q: MailboxProcessor<_>) =
            match this with
            | ClMatrix.COO m -> m.ToHost q |> Matrix.COO
            | ClMatrix.CSR m -> m.ToHost q |> Matrix.CSR
            | ClMatrix.CSC m -> m.ToHost q |> Matrix.CSC
            | ClMatrix.LIL m -> m.ToHost q |> Matrix.LIL

        member this.Free(q: MailboxProcessor<_>) =
            match this with
            | ClMatrix.COO m -> m.Free q
            | ClMatrix.CSR m -> m.Free q
            | ClMatrix.CSC m -> m.Free q
            | ClMatrix.LIL m -> m.Free q

        member this.FreeAndWait(processor: MailboxProcessor<_>) =
            this.Free processor
            processor.PostAndReply(MsgNotifyMe)

        member this.ToHostAndFree(processor: MailboxProcessor<_>) =
            let result = this.ToHost processor

            this.Free processor

            result
