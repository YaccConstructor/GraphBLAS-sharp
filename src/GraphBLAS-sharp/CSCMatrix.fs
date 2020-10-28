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
        let rowsCount = matrix |> Array2D.length1
        let columnsCount = matrix |> Array2D.length2
        
        let convertedMatrix = 
            [for i in 0 .. columnsCount - 1 -> matrix.[*, i] |> List.ofArray]
            |> List.map (fun column -> 
                column 
                |> List.mapi (fun i x -> (x, i)) 
                |> List.filter (fun pair -> fst pair |> abs > System.Double.Epsilon)
                )
            |> List.fold (fun (columnPtrs, valueInx) row -> 
                ((columnPtrs.Head + row.Length) :: columnPtrs), valueInx @ row) ([0], [])
    
        {   
            Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray
            Rows = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray 
            ColumnPointers = fst convertedMatrix |> List.rev |> List.toArray 
            RowCount = rowsCount
        }     

    let rowCount (matrix: CSCMatrix) = matrix.RowCount
    let columnCount (matrix: CSCMatrix) = matrix.ColumnPointers.Length - 1
    let nnz (matrix: CSCMatrix) = matrix.ColumnPointers.[matrix.ColumnPointers.Length - 1] 