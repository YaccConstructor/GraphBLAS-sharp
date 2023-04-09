namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open System
open Microsoft.FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Common

module internal Kronecker =
    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (op: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let mapWithValueToCOO = MapWithValue.runToCOO clContext op workGroupSize

        let mapWithValueClArray = ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        let mergeDisjointCOO = MergeDisjoint.run clContext workGroupSize

        let insertMatrixWithOffset (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'c>) rowOffset columnOffset (resultMatrix: ClMatrix.COO<'c>) =
            let newRowIndices = mapWithValueClArray queue allocationMode rowOffset matrix.Rows
            let newColumnIndices = mapWithValueClArray queue allocationMode columnOffset matrix.Columns

            let newMatrix = { Context = clContext
                              RowCount = matrix.RowCount
                              ColumnCount = matrix.ColumnCount
                              Rows = newRowIndices
                              Columns = newColumnIndices
                              Values = matrix.Values }

            mergeDisjointCOO queue resultMatrix newMatrix

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->
            let mapWithZero = mapWithValueToCOO queue allocationMode (clContext.CreateClCell None) matrixRight

            let mutable resultMatrix = mapWithZero

            for row in 0 .. (matrixLeft.RowCount - 1) do
                let NNZInRow = matrixLeft.RowPointers.[row + 1] - matrixLeft.RowPointers.[row]
                let firstElementIndex = matrixLeft.RowPointers.[row]
                let rowOffset = row * matrixRight.RowCount |> clContext.CreateClCell

                for offset in 0 .. (NNZInRow - 1) do
                    let currentColumn = matrixLeft.Columns.[firstElementIndex + offset]

                    let numberOfZeroElements =
                        match offset with
                        | 0 -> currentColumn
                        | _ -> currentColumn - matrixLeft.Columns.[firstElementIndex + offset - 1] - 1

                    let columnOffset = currentColumn * matrixRight.ColumnCount |> clContext.CreateClCell

                    // Вставляем матрицы, получившиеся в результате умножения на 0
                    if currentColumn <> 0 && numberOfZeroElements >= 0 && mapWithZero.NNZ <> 0 then
                        for i in (currentColumn - numberOfZeroElements) .. currentColumn - 1 do
                            let columnOfZeroElement = i * matrixRight.ColumnCount |> clContext.CreateClCell

                            resultMatrix <- insertMatrixWithOffset queue allocationMode mapWithZero rowOffset columnOfZeroElement resultMatrix

                    let operand = Some matrixLeft.Values.[firstElementIndex + offset] |> clContext.CreateClCell

                    let mappedMatrix = mapWithValueToCOO queue allocationMode operand matrixRight

                    resultMatrix <- insertMatrixWithOffset queue allocationMode mappedMatrix rowOffset columnOffset resultMatrix

                let lastElementInRowIndex = firstElementIndex + NNZInRow - 1
                let lastColumnInRow = matrixLeft.Columns.[lastElementInRowIndex]
                let numberOfZeroElements = matrixLeft.ColumnCount - lastColumnInRow - 1

                for i in lastElementInRowIndex + 1 .. lastElementInRowIndex + numberOfZeroElements do
                    let columnOffset = matrixLeft.Columns.[i] * matrixRight.ColumnCount |> clContext.CreateClCell

                    resultMatrix <- insertMatrixWithOffset queue allocationMode mapWithZero rowOffset columnOffset resultMatrix

            resultMatrix
