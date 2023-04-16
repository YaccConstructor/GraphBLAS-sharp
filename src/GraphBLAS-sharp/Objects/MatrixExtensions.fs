namespace GraphBLAS.FSharp.Objects

open GraphBLAS.FSharp.Backend.Objects
open Brahma.FSharp
open Matrix

module MatrixExtensions =
    type ClMatrix<'a when 'a: struct> with
        member this.ToHost(q: MailboxProcessor<_>) =
            match this with
            | ClMatrix.COO m ->
                let rows = Array.zeroCreate m.Rows.Length
                let columns = Array.zeroCreate m.Columns.Length
                let values = Array.zeroCreate m.Values.Length

                q.Post(Msg.CreateToHostMsg(m.Rows, rows))

                q.Post(Msg.CreateToHostMsg(m.Columns, columns))

                ignore
                <| q.PostAndReply(fun ch -> Msg.CreateToHostMsg(m.Values, values, ch))

                let result =
                    { RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = rows
                      Columns = columns
                      Values = values }

                Matrix.COO result
            | ClMatrix.CSR m ->
                let rows = Array.zeroCreate m.RowPointers.Length
                let columns = Array.zeroCreate m.Columns.Length
                let values = Array.zeroCreate m.Values.Length

                q.Post(Msg.CreateToHostMsg(m.RowPointers, rows))

                q.Post(Msg.CreateToHostMsg(m.Columns, columns))

                ignore
                <| q.PostAndReply(fun ch -> Msg.CreateToHostMsg(m.Values, values, ch))

                let result =
                    { RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowPointers = rows
                      ColumnIndices = columns
                      Values = values }

                Matrix.CSR result
            | ClMatrix.CSC m ->
                let rows = Array.zeroCreate m.Rows.Length
                let columns = Array.zeroCreate m.ColumnPointers.Length
                let values = Array.zeroCreate m.Values.Length

                q.Post(Msg.CreateToHostMsg(m.Rows, rows))

                q.Post(Msg.CreateToHostMsg(m.ColumnPointers, columns))

                ignore
                <| q.PostAndReply(fun ch -> Msg.CreateToHostMsg(m.Values, values, ch))

                let result =
                    { RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowIndices = rows
                      ColumnPointers = columns
                      Values = values }

                Matrix.CSC result
            | _ -> failwith "Not yet implemented"
