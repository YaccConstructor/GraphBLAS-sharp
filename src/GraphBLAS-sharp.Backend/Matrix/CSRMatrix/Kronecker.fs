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
    type private optionalMatrix<'elem when 'elem: struct> =
        { RowCount: int
          ColumnCount: int
          Matrix: (ClArray<int> * ClArray<int> * ClArray<'elem>) option }

    let private insertMatrixWithOffset (clContext: ClContext) workGroupSize =

        let mapWithValueClArray =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO =
            MergeDisjoint.run clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode rowOffset columnOffset (matrixToInsert: optionalMatrix<'c>) (resultMatrix: optionalMatrix<'c>) ->

            let newColCount =
                if resultMatrix.ColumnCount < columnOffset + matrixToInsert.ColumnCount then
                    columnOffset + matrixToInsert.ColumnCount
                else
                    resultMatrix.ColumnCount

            let newRowCount =
                if resultMatrix.RowCount < rowOffset + matrixToInsert.RowCount then
                    rowOffset + matrixToInsert.RowCount
                else
                    resultMatrix.RowCount

            match matrixToInsert.Matrix with
            | None ->
                { resultMatrix with
                      ColumnCount = newColCount
                      RowCount = newRowCount }

            | Some (rows, cols, vals) ->
                let newRowIndices =
                    mapWithValueClArray queue allocationMode (rowOffset |> clContext.CreateClCell) rows

                let newColumnIndices =
                    mapWithValueClArray queue allocationMode (columnOffset |> clContext.CreateClCell) cols

                match resultMatrix.Matrix with
                | None ->
                    { RowCount = newColCount
                      ColumnCount = newColCount
                      Matrix = Some (newRowIndices, newColumnIndices, vals) }

                | Some (mainRows, mainCols, mainVals) ->
                    let newMatrixCOO =
                        { Context = clContext
                          RowCount = matrixToInsert.RowCount
                          ColumnCount = matrixToInsert.ColumnCount
                          Rows = newRowIndices
                          Columns = newColumnIndices
                          Values = vals }

                    let mainMatrixCOO =
                        { Context = clContext
                          RowCount = resultMatrix.RowCount
                          ColumnCount = resultMatrix.ColumnCount
                          Rows = mainRows
                          Columns = mainCols
                          Values = mainVals }

                    let newMatrix =
                        mergeDisjointCOO queue mainMatrixCOO newMatrixCOO

                    { RowCount = newColCount
                      ColumnCount = newColCount
                      Matrix = Some (newMatrix.Rows, newMatrix.Columns, newMatrix.Values) }

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
                    { RowCount = matrixRight.RowCount
                      ColumnCount = matrixRight.RowCount
                      Matrix = None }
                | Some m ->
                    { RowCount = matrixRight.RowCount
                      ColumnCount = matrixRight.RowCount
                      Matrix = Some (m.Rows, m.Columns, m.Values) }

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
                                insert queue allocationMode rowOffset columnOfZeroElement mapWithZero resultMatrix

                    let operand =
                        Some leftMatrixVals.[firstElementIndex + offset]
                        |> clContext.CreateClCell

                    let mappedMatrix =
                        match mapWithValueToCOO queue allocationMode operand matrixRight with
                        | None ->
                            { RowCount = matrixRight.RowCount
                              ColumnCount = matrixRight.RowCount
                              Matrix = None }
                        | Some m ->
                            { RowCount = m.RowCount
                              ColumnCount = m.RowCount
                              Matrix = Some (m.Rows, m.Columns, m.Values) }

                    resultMatrix <- insert queue allocationMode rowOffset columnOffset mappedMatrix resultMatrix

                let startColumn =
                    match NNZInRow with
                    | 0 -> 0
                    | _ ->
                        leftMatrixCols.[firstElementIndex + NNZInRow - 1]
                        + 1

                for i in startColumn .. matrixLeft.ColumnCount - 1 do
                    let columnOffset = i * matrixRight.ColumnCount

                    resultMatrix <- insert queue allocationMode rowOffset columnOffset mapWithZero resultMatrix

            match resultMatrix.Matrix with
            | None -> None
            | Some (rows, cols, vals) ->
                Some
                    { Context = clContext
                      RowCount = matrixLeft.RowCount * matrixRight.RowCount
                      ColumnCount = matrixLeft.ColumnCount * matrixRight.ColumnCount
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
            | Some resultMatrix -> Some (resultMatrix |> toCSRInplace queue allocationMode)
