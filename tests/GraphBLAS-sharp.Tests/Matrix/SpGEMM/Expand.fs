module GraphBLAS.FSharp.Tests.Backend.Matrix.SpGEMM.Expand

open GraphBLAS.FSharp.Objects.Matrix
open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Expecto

let context = Context.defaultContext

/// <remarks>
/// Left matrix
/// </remarks>
/// <code>
/// [ 0 0 2 3 0
///   0 0 0 0 0
///   0 8 0 5 4
///   0 0 2 0 0
///   1 7 0 0 0 ]
/// </code>
let leftMatrix =
    { RowCount = 5
      ColumnCount = 5
      RowPointers = [| 0; 2; 2; 5; 6; 8 |]
      ColumnIndices = [| 2; 3; 1; 3; 4; 2; 0; 1|]
      Values = [| 2; 3; 8; 5; 4; 2; 1; 7 |] }

/// <remarks>
/// Right matrix
/// </remarks>
/// <code>
/// [ 0 0 0 0 0 0 0
///   0 3 0 0 4 0 4
///   0 0 2 0 0 2 0
///   0 5 0 0 0 9 1
///   0 0 0 0 1 0 8 ]
/// </code>
let rightMatrix =
    { RowCount = 5
      ColumnCount = 7
      RowPointers = [| 0; 0; 3; 5; 8; 10 |]
      ColumnIndices = [| 1; 4; 6; 2; 5; 1; 5; 6; 4; 6 |]
      Values = [| 3; 4; 4; 2; 2; 5; 9; 1; 1; 8 |] }

let requiredRowLength =
    testCase "requiredRowLength"
    <| fun () ->
        let clContext = context.ClContext
        let processor = context.Queue

        let deviceLeftMatrix = leftMatrix.ToDevice clContext
        let deviceRightMatrix = rightMatrix.ToDevice clContext

        let getRequiredRawsLengths =
            Expand.processLeftMatrixColumnsAndRightMatrixRawPointers clContext Utils.defaultWorkGroupSize Expand.requiredRawsLengths

        let requiredRawsLengths =
            getRequiredRawsLengths processor deviceLeftMatrix.Columns deviceRightMatrix.RowPointers

        let requiredRawsLengthsHost = requiredRawsLengths.ToHost processor

        "Results must be the same"
        |> Expect.equal requiredRawsLengthsHost [| 2; 3; 3; 3; 2; 2; 0; 3 |]



