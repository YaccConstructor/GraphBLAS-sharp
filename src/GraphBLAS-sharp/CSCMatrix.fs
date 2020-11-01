namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions

// type CSCMatrix<'a>(denseMatrix: 'a[,]) =
//     inherit Matrix<'a>()

//     let rowsCount = denseMatrix |> Array2D.length1
//     let columnsCount = denseMatrix |> Array2D.length2

//     let convertedMatrix =
//         [for i in 0 .. columnsCount - 1 -> denseMatrix.[*, i] |> List.ofArray]
//         |> List.map (fun column ->
//             column
//             |> List.mapi (fun i x -> (x, i))
//             // |> List.filter (fun pair -> fst pair |> abs > System.Double.Epsilon)
//             )
//         |> List.fold (fun (columnPtrs, valueInx) row ->
//             ((columnPtrs.Head + row.Length) :: columnPtrs), valueInx @ row) ([0], [])

//     member this.Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray
//     member this.Rows = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray
//     member this.ColumnPointers = fst convertedMatrix |> List.rev |> List.toArray

//     override this.RowCount = rowsCount
//     override this.ColumnCount = this.ColumnPointers.Length - 1

// module CSСMatrix =
//     let ofDense (matrix: 'a[,]) = CSCMatrix(matrix)
//     let rowCount (matrix: CSCMatrix<'a>) = matrix.RowCount
//     let columnCount (matrix: CSCMatrix<'a>) = matrix.ColumnCount
//     let nnz (matrix: CSCMatrix<'a>) = matrix.ColumnPointers.[matrix.ColumnPointers.Length - 1]
