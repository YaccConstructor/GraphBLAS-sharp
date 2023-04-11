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
    let runToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let mapWithValueToCOO =
            MapWithValue.runToCOO clContext op workGroupSize

        let mapWithValueClArray =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO =
            MergeDisjoint.run clContext workGroupSize

        let insertMatrixWithOffset
            (queue: MailboxProcessor<_>)
            allocationMode
            (matrix: ClMatrix.COO<'c> option)
            rowOffset
            columnOffset
            (resultMatrix: ClMatrix.COO<'c> option)
            =

            match matrix with
            | None ->
                match resultMatrix with
                | None -> // TODO: нужно возрвращать нулевую матрицу определенного размера
            | Some cooMatrix ->
                let newRowIndices =
                    mapWithValueClArray queue allocationMode rowOffset cooMatrix.Rows

                let newColumnIndices =
                    mapWithValueClArray queue allocationMode columnOffset cooMatrix.Columns

                let newMatrix =
                    { Context = clContext
                      RowCount = cooMatrix.RowCount
                      ColumnCount = cooMatrix.ColumnCount
                      Rows = newRowIndices
                      Columns = newColumnIndices
                      Values = cooMatrix.Values }

                match resultMatrix with
                | None -> Some newMatrix
                | Some mainMatrix -> Some (mergeDisjointCOO queue mainMatrix newMatrix)

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let mapWithZero =
                mapWithValueToCOO queue allocationMode (clContext.CreateClCell(None)) matrixRight

            let mutable resultMatrix = mapWithZero

            let leftMatrixRows = matrixLeft.RowPointers.ToHost queue
            let leftMatrixCols = matrixLeft.Columns.ToHost queue
            let leftMatrixVals = matrixLeft.Values.ToHost queue

            for row in 0 .. (matrixLeft.RowCount - 1) do
                let NNZInRow =
                    leftMatrixRows.[row + 1] - leftMatrixRows.[row]

                let firstElementIndex = leftMatrixRows.[row]

                let rowOffset =
                    row * matrixRight.RowCount
                    |> clContext.CreateClCell

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

                    let columnOffset =
                        currentColumn * matrixRight.ColumnCount
                        |> clContext.CreateClCell

                    // Вставляем матрицы, получившиеся в результате умножения на 0
                    if currentColumn <> 0
                       && numberOfZeroElements >= 0
                       && mapWithZero <> None then
                        for i in (currentColumn - numberOfZeroElements) .. currentColumn - 1 do
                            let columnOfZeroElement =
                                i * matrixRight.ColumnCount
                                |> clContext.CreateClCell

                            resultMatrix <-
                                insertMatrixWithOffset
                                    queue
                                    allocationMode
                                    mapWithZero
                                    rowOffset
                                    columnOfZeroElement
                                    resultMatrix

                    let operand =
                        Some leftMatrixVals.[firstElementIndex + offset]
                        |> clContext.CreateClCell

                    let mappedMatrix =
                        mapWithValueToCOO queue allocationMode operand matrixRight

                    resultMatrix <-
                        insertMatrixWithOffset queue allocationMode mappedMatrix rowOffset columnOffset resultMatrix

                let startColumn =
                    match NNZInRow with
                    | 0 -> 0
                    | _ ->
                        leftMatrixCols.[firstElementIndex + NNZInRow - 1]
                        + 1

                for i in startColumn .. matrixLeft.ColumnCount - 1 do
                    let columnOffset =
                        i * matrixRight.ColumnCount
                        |> clContext.CreateClCell

                    resultMatrix <-
                        insertMatrixWithOffset queue allocationMode mapWithZero rowOffset columnOffset resultMatrix

            resultMatrix

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let kroneckerToCOO = runToCOO clContext op workGroupSize

        let toCSRInplace =
            Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let result = kroneckerToCOO queue allocationMode matrixLeft matrixRight
            match result with
            | None -> None
            | Some resultMatrix -> Some (resultMatrix |> toCSRInplace queue allocationMode)
