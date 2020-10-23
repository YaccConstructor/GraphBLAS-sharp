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
        static member FloatMatrix () =
            fun size -> 
                Gen.choose (-1000, 1000) 
                |> Gen.map float
                |> Gen.array2DOf
            |> Gen.sized
            |> Arb.fromGen

    [<Property(Arbitrary=[| typeof<FloatMatrix> |])>]
    let ``Аfter inverse transformation matrix should be original`` (matrix: float[,]) = 
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
        
        matrix |> CSRMatrix.makeFromDenseMatrix |> makeDenseFromCsr = matrix