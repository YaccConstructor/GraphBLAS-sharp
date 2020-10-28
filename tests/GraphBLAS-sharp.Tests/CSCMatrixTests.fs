namespace CSRMultiplication.Tests

open System
open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open CSRMultiplication
open CSRMatrixTests

[<Properties(Verbose=true, MaxTest=100, EndSize=2500)>]
module CSCMatrixTests = 

    [<Property(Arbitrary=[| typeof<FloatMatrix> |])>]
    let ``Matrix should be original after inverse fromCsr transformation`` (matrix: float[,]) = 
        let makeDenseFromCsc (matrix: CSCMatrix.CSCMatrix) = 
            let rowCount = matrix |> CSCMatrix.rowCount
            let columnCount = matrix |> CSCMatrix.columnCount
            let bufferMatrix = Array2D.zeroCreate<float> rowCount columnCount
            for columnIdx in 0 .. columnCount - 1 do 
                for i in matrix.GetColumnPointers.[columnIdx] .. matrix.GetColumnPointers.[columnIdx + 1] - 1 do
                    let rowIdx = matrix.GetRows.[i]
                    let value = matrix.GetValues.[i]
                    bufferMatrix.[rowIdx, columnIdx] <- value 
            bufferMatrix
        
        let result = matrix |> CSCMatrix.makeFromDenseMatrix |> makeDenseFromCsc 
        result = matrix |@ (sprintf "\n %A \n %A \n" result matrix)