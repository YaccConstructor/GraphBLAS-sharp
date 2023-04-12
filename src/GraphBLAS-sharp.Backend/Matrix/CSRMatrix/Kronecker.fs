namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Microsoft.FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module internal Kronecker =
    let insertMatrixWithOffset (clContext: ClContext) workGroupSize =

        let mapWithValueClArray =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO =
            MergeDisjoint.run clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixToInsert: {| Matrix: (ClArray<int> * ClArray<int> * ClArray<'c>) option
                                                                            ColCount: int
                                                                            RowCount: int |}) rowOffset columnOffset (resultMatrix: {| Matrix: (ClArray<int> * ClArray<int> * ClArray<'c>) option
                                                                                                                                       ColCount: int
                                                                                                                                       RowCount: int |}) ->

            let newColCount =
                if resultMatrix.ColCount < columnOffset + matrixToInsert.ColCount then
                    columnOffset + matrixToInsert.ColCount
                else
                    resultMatrix.ColCount

            let newRowCount =
                if resultMatrix.RowCount < rowOffset + matrixToInsert.RowCount then
                    rowOffset + matrixToInsert.RowCount
                else
                    resultMatrix.RowCount

            match matrixToInsert.Matrix with
            | None ->
                {| resultMatrix with
                       ColCount = newColCount
                       RowCount = newRowCount |}

            | Some (rows, cols, vals) ->
                let newRowIndices =
                    mapWithValueClArray queue allocationMode (rowOffset |> clContext.CreateClCell) rows

                let newColumnIndices =
                    mapWithValueClArray queue allocationMode (columnOffset |> clContext.CreateClCell) cols

                match resultMatrix.Matrix with
                | None ->
                    {| Matrix = Some(newRowIndices, newColumnIndices, vals)
                       RowCount = newColCount
                       ColCount = newColCount |}

                | Some (mainRows, mainCols, mainVals) ->
                    let newMatrix =
                        { Context = clContext
                          RowCount = matrixToInsert.RowCount
                          ColumnCount = matrixToInsert.ColCount
                          Rows = newRowIndices
                          Columns = newColumnIndices
                          Values = vals }

                    let mainMatrixCOO =
                        { Context = clContext
                          RowCount = resultMatrix.RowCount
                          ColumnCount = resultMatrix.ColCount
                          Rows = mainRows
                          Columns = mainCols
                          Values = mainVals }

                    let newMatrix =
                        mergeDisjointCOO queue mainMatrixCOO newMatrix

                    {| Matrix = Some(newMatrix.Rows, newMatrix.Columns, newMatrix.Values)
                       RowCount = newColCount
                       ColCount = newColCount |}


    let runToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let mapWithValueToCOO =
            MapWithValue.runToCOO clContext op workGroupSize

        let insert =
            insertMatrixWithOffset clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let mapWithZero =
                match mapWithValueToCOO queue allocationMode (clContext.CreateClCell(None)) matrixRight with
                | None ->
                    {| Matrix = None
                       RowCount = matrixRight.RowCount
                       ColCount = matrixRight.RowCount |}
                | Some m ->
                    {| Matrix = Some(m.Rows, m.Columns, m.Values)
                       RowCount = matrixRight.RowCount
                       ColCount = matrixRight.RowCount |}

            let mutable resultMatrix = mapWithZero

            let leftMatrixRows = matrixLeft.RowPointers.ToHost queue
            let leftMatrixCols = matrixLeft.Columns.ToHost queue
            let leftMatrixVals = matrixLeft.Values.ToHost queue

            for row in 0 .. (matrixLeft.RowCount - 1) do
                let NNZInRow =
                    leftMatrixRows.[row + 1] - leftMatrixRows.[row]

                let firstElementIndex = leftMatrixRows.[row]

                let rowOffset = row * matrixRight.RowCount

                for offset in 0 .. (NNZInRow - 1) do
                    let currentColumn =
                        leftMatrixCols.[firstElementIndex + offset]

                    let numberOfZeroElements =
                        match offset with
                        | 0 -> currentColumn
                        | _ ->
                            currentColumn
                            - leftMatrixCols.[firstElementIndex + offset - 1]
                            - 1

                    let columnOffset = currentColumn * matrixRight.ColumnCount

                    // Вставляем матрицы, получившиеся в результате умножения на 0
                    if currentColumn <> 0 && numberOfZeroElements >= 0 then
                        for i in (currentColumn - numberOfZeroElements) .. currentColumn - 1 do
                            let columnOfZeroElement = i * matrixRight.ColumnCount

                            resultMatrix <-
                                insert queue allocationMode mapWithZero rowOffset columnOfZeroElement resultMatrix

                    let operand =
                        Some leftMatrixVals.[firstElementIndex + offset]
                        |> clContext.CreateClCell

                    let mappedMatrix =
                        match mapWithValueToCOO queue allocationMode operand matrixRight with
                        | None ->
                            {| Matrix = None
                               RowCount = matrixRight.RowCount
                               ColCount = matrixRight.RowCount |}
                        | Some m ->
                            {| Matrix = Some(m.Rows, m.Columns, m.Values)
                               RowCount = m.RowCount
                               ColCount = m.RowCount |}

                    resultMatrix <- insert queue allocationMode mappedMatrix rowOffset columnOffset resultMatrix

                let startColumn =
                    match NNZInRow with
                    | 0 -> 0
                    | _ ->
                        leftMatrixCols.[firstElementIndex + NNZInRow - 1]
                        + 1

                for i in startColumn .. matrixLeft.ColumnCount - 1 do
                    let columnOffset = i * matrixRight.ColumnCount

                    resultMatrix <- insert queue allocationMode mapWithZero rowOffset columnOffset resultMatrix

            match resultMatrix.Matrix with
            | None -> None
            | Some (rows, cols, vals) ->
                Some
                    { Context = clContext
                      RowCount = resultMatrix.RowCount
                      ColumnCount = resultMatrix.ColCount
                      Rows = rows
                      Columns = cols
                      Values = vals }

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let kroneckerToCOO = runToCOO clContext op workGroupSize

        let toCSRInplace =
            Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let result =
                kroneckerToCOO queue allocationMode matrixLeft matrixRight

            match result with
            | None -> None
            | Some resultMatrix -> Some(resultMatrix |> toCSRInplace queue allocationMode)
