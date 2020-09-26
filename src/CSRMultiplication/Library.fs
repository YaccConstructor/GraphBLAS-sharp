namespace CSRMultiplication

module CSRMatrix = 
    type CSRMatrix = private {
        ValueColumnPairs: (float * int) array
        RowPointers: int array
    } 
    with 
        member this.GetValueColumnPair idx = this.ValueColumnPairs.[idx]
        member this.GetRowPointer idx = this.RowPointers.[idx]

    let makeFromDenseMatrix (matrix: float[,]) = 
        let rowsCount = Array2D.length1 matrix
        let columnsCount = Array2D.length2 matrix
        
        let convertedMatrix = 
            [for i in 0 .. rowsCount - 1 -> matrix.[i, *] |> List.ofArray]
            |> List.map (fun row -> 
                row 
                |> List.mapi (fun i x -> (x, i)) 
                |> List.filter (fun pair -> abs (fst pair) > 0.0000001 )
                )
            |> List.fold (fun (rowPtrs, valueInx) row -> 
                ((rowPtrs.Head + row.Length) :: rowPtrs), valueInx @ row) ([0], [])
    
        {   
            ValueColumnPairs = snd convertedMatrix |> List.toArray; 
            RowPointers = fst convertedMatrix |> List.rev |> List.toArray 
        }

    let rowCount (matrix: CSRMatrix) = matrix.RowPointers.Length - 1
        
// module CSCMatrix = 
//     type CSCMatrix = private {
//         ValueRowPairs: (float * int) array
//         ColumnPointers: int array
//     }

//     let makeFromDenseMatrix (matrix: float[,]) = 
//         let rowsCount = Array2D.length1 matrix
//         let columnsCount = Array2D.length2 matrix
        
//         let convertedMatrix = 
//             [for i in 0 .. columnsCount - 1 -> matrix.[*, i] |> List.ofArray]
//             |> List.map (fun column -> 
//                 column 
//                 |> List.mapi (fun i x -> (x, i)) 
//                 |> List.filter (fun pair -> abs (fst pair) > 0.0000001 )
//                 )
//             |> List.fold (fun (columnPtrs, valueInx) row -> 
//                 ((columnPtrs.Head + row.Length) :: columnPtrs), valueInx @ row) ([0], [])
    
//         {   
//             ValueRowPairs = snd convertedMatrix |> List.toArray; 
//             ColumnPointers = fst convertedMatrix |> List.rev |> List.toArray 
//         }

module SparseMatrixOperations = 
    let multiply (a: CSRMatrix.CSRMatrix) (b: float[]) = 
        let rowCount = CSRMatrix.rowCount a
        let c = Array.zeroCreate<float> rowCount
        for i in 0 .. rowCount - 1 do
            for j in a.GetRowPointer i .. a.GetRowPointer (i + 1) - 1 do
                let vcPair = a.GetValueColumnPair j
                c.[i] <- c.[i] + fst vcPair * b.[snd vcPair]
        
        c