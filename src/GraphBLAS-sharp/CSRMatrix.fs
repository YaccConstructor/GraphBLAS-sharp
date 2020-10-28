namespace CSRMultiplication

module CSRMatrix = 
    type CSRMatrix = private {
        Values: float[]
        Columns: int[]
        RowPointers: int[]
        ColumnCount: int
    } 
    with 
        member this.GetValues = this.Values
        member this.GetColumns = this.Columns
        member this.GetRowPointers = this.RowPointers

    let makeFromDenseMatrix (matrix: float[,]) = 
        let rowsCount = matrix |> Array2D.length1
        let columnsCount = matrix |> Array2D.length2
        
        let convertedMatrix = 
            [for i in 0 .. rowsCount - 1 -> matrix.[i, *] |> List.ofArray]
            |> List.map (fun row -> 
                row 
                |> List.mapi (fun i x -> (x, i)) 
                |> List.filter (fun pair -> fst pair |> abs > System.Double.Epsilon)
                )
            |> List.fold (fun (rowPtrs, valueInx) row -> 
                ((rowPtrs.Head + row.Length) :: rowPtrs), valueInx @ row) ([0], [])
    
        {   
            Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray
            Columns = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray
            RowPointers = convertedMatrix |> fst |> List.rev |> List.toArray
            ColumnCount = columnsCount
        }

    let rowCount (matrix: CSRMatrix) = matrix.RowPointers.Length - 1
    let columnCount (matrix: CSRMatrix) = matrix.ColumnCount
    let nnz (matrix: CSRMatrix) = matrix.RowPointers.[matrix.RowPointers.Length - 1]    