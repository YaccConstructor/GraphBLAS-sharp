namespace CSRMultiplication

module CSCMatrix = 
    type CSCMatrix = private {
        Values: float[]
        Rows: int[]
        ColumnPointers: int[]
        RowCount: int
    }
    with 
        member this.GetValues = this.Values
        member this.GetRows = this.Rows
        member this.GetColumnPointers = this.ColumnPointers

    let makeFromDenseMatrix (matrix: float[,]) = 
        let rowsCount = Array2D.length1 matrix
        let columnsCount = Array2D.length2 matrix
        
        let delta = 1e-8
        let convertedMatrix = 
            [for i in 0 .. columnsCount - 1 -> matrix.[*, i] |> List.ofArray]
            |> List.map (fun column -> 
                column 
                |> List.mapi (fun i x -> (x, i)) 
                |> List.filter (fun pair -> fst pair |> abs > delta)
                )
            |> List.fold (fun (columnPtrs, valueInx) row -> 
                ((columnPtrs.Head + row.Length) :: columnPtrs), valueInx @ row) ([0], [])
    
        {   
            Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray
            Rows = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray 
            ColumnPointers = fst convertedMatrix |> List.rev |> List.toArray 
            RowCount = rowsCount
        }     

    let rowCount (matrix: CSCMatrix) = matrix.ColumnPointers.Length - 1
    let columnCount (matrix: CSCMatrix) = matrix.RowCount
    let nnz (matrix: CSCMatrix) = matrix.ColumnPointers.[^1] 