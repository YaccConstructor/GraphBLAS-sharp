namespace CSRMultiplication

open System
open OpenCL.Net
open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Microsoft.FSharp.Quotations
open Brahma.FSharp.OpenCL.Extensions

module CSRMatrix = 
    type CSRMatrix = private {
        Values: float[]
        Columns: int[]
        RowPointers: int[]
    } 
    with 
        member this.GetValues = this.Values
        member this.GetColumns = this.Columns
        member this.GetRowPointers = this.RowPointers

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
            Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray; 
            Columns = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray;
            RowPointers = convertedMatrix |> fst |> List.rev |> List.toArray 
        }

    let rowCount (matrix: CSRMatrix) = matrix.RowPointers.Length - 1
        
module SparseMatrixOperations = 
    let multiply (provider: ComputeProvider) (commandQueue: CommandQueue) (vector: float[]) (matrix: CSRMatrix.CSRMatrix) = 
        let rowCount = CSRMatrix.rowCount matrix
        let resultVector = Array.zeroCreate<float> rowCount
        let command = 
            <@
                fun (ndRange: _1D)
                    (resultBuffer: array<_>) 
                    (valuesBuffer: array<_>) 
                    (columnsBuffer: array<_>) 
                    (rowPointersBuffer: array<_>) 
                    (vectorBuffer: array<_>) ->

                    let i = ndRange.GlobalID0
                    for j in rowPointersBuffer.[i] .. rowPointersBuffer.[i + 1] - 1 do
                        resultBuffer.[i] <- resultBuffer.[i] + valuesBuffer.[j] * vectorBuffer.[columnsBuffer.[j]]       
            @>

        let (kernel, kernelPrepare, kernelRun) = provider.Compile command 
        kernelPrepare (_1D rowCount) resultVector matrix.GetValues matrix.GetColumns matrix.GetRowPointers vector
        commandQueue.Add(kernelRun()).Finish() |> ignore
        commandQueue.Add(resultVector.ToHost provider).Finish() |> ignore

        resultVector
        

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