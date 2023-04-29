namespace GraphBLAS.FSharp.Objects

open GraphBLAS.FSharp.Backend.Objects
open Brahma.FSharp
open Matrix
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClVectorExtensions

module MatrixExtensions =
    type ClMatrix<'a when 'a: struct> with
        member this.ToHost(q: MailboxProcessor<_>) =
            match this with
            | ClMatrix.COO m ->
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows = m.Rows.ToHost q
                  Columns = m.Columns.ToHost q
                  Values = m.Values.ToHost q }
                |> Matrix.COO
            | ClMatrix.CSR m ->
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowPointers = m.RowPointers.ToHost q
                  ColumnIndices = m.Columns.ToHost q
                  Values = m.Values.ToHost q }
                |> Matrix.CSR
            | ClMatrix.CSC m ->
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowIndices = m.Rows.ToHost q
                  ColumnPointers = m.ColumnPointers.ToHost q
                  Values = m.Values.ToHost q }
                |> Matrix.CSC
            | ClMatrix.LIL m ->
                { RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows =
                      m.Rows
                      |> List.map (Option.map (fun row -> row.ToHost q))
                  NNZ = m.NNZ }
                |> Matrix.LIL

        member this.ToHostAndDispose(processor: MailboxProcessor<_>) =
            let result = this.ToHost processor

            this.Dispose processor

            result
