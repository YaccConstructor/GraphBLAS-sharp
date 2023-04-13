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

        let copyClArray = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (rowOffset: int) (columnOffset: int) (matrixToInsert: optionalMatrix<'c>) (resultMatrix: optionalMatrix<'c>) ->

            let newRowCount =
                if resultMatrix.RowCount < rowOffset + matrixToInsert.RowCount then
                    rowOffset + matrixToInsert.RowCount
                else
                    resultMatrix.RowCount

            let newColCount =
                if resultMatrix.ColumnCount < columnOffset + matrixToInsert.ColumnCount then
                    columnOffset + matrixToInsert.ColumnCount
                else
                    resultMatrix.ColumnCount

            match matrixToInsert.Matrix with
            | None ->
                { resultMatrix with
                      RowCount = newRowCount
                      ColumnCount = newColCount }

            | Some (rows, cols, vals) ->
                let rowOffset = rowOffset |> clContext.CreateClCell
                let columnOffset = columnOffset |> clContext.CreateClCell

                let newRowIndices =
                    mapWithValueClArray queue allocationMode rowOffset rows

                let newColumnIndices =
                    mapWithValueClArray queue allocationMode columnOffset cols

                queue.Post(Msg.CreateFreeMsg<_>(rowOffset))
                queue.Post(Msg.CreateFreeMsg<_>(columnOffset))

                match resultMatrix.Matrix with
                | None ->
                    { RowCount = newRowCount
                      ColumnCount = newColCount
                      Matrix = Some(newRowIndices, newColumnIndices, copyClArray queue allocationMode vals) }

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
                          RowCount = newRowCount
                          ColumnCount = newColCount
                          Rows = mainRows
                          Columns = mainCols
                          Values = mainVals }

                    let newMatrix =
                        mergeDisjointCOO queue mainMatrixCOO newMatrixCOO

                    { RowCount = newRowCount
                      ColumnCount = newColCount
                      Matrix = Some(newMatrix.Rows, newMatrix.Columns, newMatrix.Values) }

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
            let noneClCell = None |> clContext.CreateClCell

            let mapWithZero =
                match mapWithValueToCOO queue allocationMode noneClCell matrixRight with
                | None ->
                    { RowCount = matrixRight.RowCount
                      ColumnCount = matrixRight.RowCount
                      Matrix = None }
                | Some m ->
                    { RowCount = matrixRight.RowCount
                      ColumnCount = matrixRight.RowCount
                      Matrix = Some(m.Rows, m.Columns, m.Values) }

            queue.Post(Msg.CreateFreeMsg<_>(noneClCell))

            let mutable resultMatrix =
                { RowCount = 0
                  ColumnCount = 0
                  Matrix = None }

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

                    // Inserting matrices resulting from multiplication by 0
                    if currentColumn <> 0 && numberOfZeroElements >= 0 then
                        for i in (currentColumn - numberOfZeroElements) .. currentColumn - 1 do
                            let columnOffset = i * matrixRight.ColumnCount

                            let newMatrix =
                                insert queue allocationMode rowOffset columnOffset mapWithZero resultMatrix

                            resultMatrix <-
                                match resultMatrix.Matrix, mapWithZero.Matrix with
                                | Some (oldRows, oldCols, oldVals), Some _ ->
                                    queue.Post(Msg.CreateFreeMsg<_>(oldRows))
                                    queue.Post(Msg.CreateFreeMsg<_>(oldCols))
                                    queue.Post(Msg.CreateFreeMsg<_>(oldVals))
                                    newMatrix
                                | _ -> newMatrix

                    // inserting new matrix resulting from multiplication by non None element of left matrix
                    let columnOffset = currentColumn * matrixRight.ColumnCount

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
                              Matrix = Some(m.Rows, m.Columns, m.Values) }

                    queue.Post(Msg.CreateFreeMsg<_>(operand))

                    let newMatrix =
                        insert queue allocationMode rowOffset columnOffset mappedMatrix resultMatrix

                    resultMatrix <-
                        match resultMatrix.Matrix, mappedMatrix.Matrix with
                        | Some (oldRows, oldCols, oldVals), Some (mappedRows, mappedCols, mappedVals) ->
                            queue.Post(Msg.CreateFreeMsg<_>(oldRows))
                            queue.Post(Msg.CreateFreeMsg<_>(oldCols))
                            queue.Post(Msg.CreateFreeMsg<_>(oldVals))
                            queue.Post(Msg.CreateFreeMsg<_>(mappedRows))
                            queue.Post(Msg.CreateFreeMsg<_>(mappedCols))
                            queue.Post(Msg.CreateFreeMsg<_>(mappedVals))
                            newMatrix
                        | _ -> newMatrix

                // inserting matrices resulting from multiplication by 0 to the rest of the place in row
                let startColumn =
                    match NNZInRow with
                    | 0 -> 0
                    | _ ->
                        leftMatrixCols.[firstElementIndex + NNZInRow - 1]
                        + 1

                for i in startColumn .. matrixLeft.ColumnCount - 1 do
                    let columnOffset = i * matrixRight.ColumnCount

                    let newMatrix =
                        insert queue allocationMode rowOffset columnOffset mapWithZero resultMatrix

                    resultMatrix <-
                        match resultMatrix.Matrix, mapWithZero.Matrix with
                        | Some (oldRows, oldCols, oldVals), Some _ ->
                            queue.Post(Msg.CreateFreeMsg<_>(oldRows))
                            queue.Post(Msg.CreateFreeMsg<_>(oldCols))
                            queue.Post(Msg.CreateFreeMsg<_>(oldVals))
                            newMatrix
                        | _ -> newMatrix

            match resultMatrix.Matrix with
            | None -> None
            | Some (rows, cols, vals) ->
                Some
                    { Context = clContext
                      RowCount = matrixLeft.RowCount * matrixRight.RowCount // resultMatrix.RowCount
                      ColumnCount = matrixLeft.ColumnCount * matrixRight.ColumnCount // resultMatrix.ColumnCount
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
