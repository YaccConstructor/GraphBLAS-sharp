namespace CSRMultiplication.Tests

open System
open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open GraphBLAS.FSharp

// [<Properties(Verbose=true, MaxTest=100, EndSize=2500)>]
module CSRMatrixTests =
    ()

//     type FloatMatrix =
//         static member FloatSparseMatrix () =
//             Gen.oneof [
//                 Arb.Default.NormalFloat () |> Arb.toGen |> Gen.map float
//                 Gen.constant 0.
//             ]
//             |> Gen.array2DOf
//             |> Arb.fromGen

//     [<Property(Arbitrary=[| typeof<FloatMatrix> |])>]
//     let ``Matrix should be original after inverse fromCsr transformation`` (matrix: float[,]) =
//         let makeDenseFromCsr (matrix: CSRMatrix<float>) =
//             let rowCount = matrix.RowCount
//             let columnCount = matrix.ColumnCount
//             let bufferMatrix = Array2D.zeroCreate<float> rowCount columnCount
//             for rowIdx in 0 .. rowCount - 1 do
//                 for i in matrix.RowPointers.[rowIdx] .. matrix.RowPointers.[rowIdx + 1] - 1 do
//                     let columnIdx = matrix.Columns.[i]
//                     let value = matrix.Values.[i]
//                     bufferMatrix.[rowIdx, columnIdx] <- value
//             bufferMatrix

//         let result = matrix |> CSRMatrix.ofDense |> makeDenseFromCsr
//         result = matrix |@ (sprintf "\n %A \n %A \n" result matrix)
