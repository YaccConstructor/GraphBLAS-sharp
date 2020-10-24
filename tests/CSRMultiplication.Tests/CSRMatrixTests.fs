namespace CSRMultiplication.Tests

open System
open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open CSRMultiplication

[<Properties(Verbose=true, MaxTest=100, EndSize=2500)>]
module CSRMatrixTests = 

    type FloatMatrix = 
        static member FloatSparseMatrix () =
            Gen.oneof [
                Arb.Default.NormalFloat () |> Arb.toGen |> Gen.map float
                Gen.constant 0.
            ]
            |> Gen.array2DOf
            |> Arb.fromGen

    [<Property(Arbitrary=[| typeof<FloatMatrix> |])>]
    let ``Matrix should be original after inverse fromCsr transformation`` (matrix: float[,]) = 
        let makeDenseFromCsr (matrix: CSRMatrix.CSRMatrix) = 
            let rowCount = matrix |> CSRMatrix.rowCount
            let columnCount = matrix |> CSRMatrix.columnCount
            let bufferMatrix = Array2D.zeroCreate<float> rowCount columnCount
            for rowIdx in 0 .. rowCount - 1 do 
                for i in matrix.GetRowPointers.[rowIdx] .. matrix.GetRowPointers.[rowIdx + 1] - 1 do
                    let columnIdx = matrix.GetColumns.[i]
                    let value = matrix.GetValues.[i]
                    bufferMatrix.[rowIdx, columnIdx] <- value 
            bufferMatrix
        
        let result = matrix |> CSRMatrix.makeFromDenseMatrix |> makeDenseFromCsr
        result = matrix |@ (sprintf "\n %A \n %A \n" result matrix)