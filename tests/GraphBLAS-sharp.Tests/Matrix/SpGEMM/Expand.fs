module GraphBLAS.FSharp.Tests.Backend.Matrix.SpGEMM.Expand

open GraphBLAS.FSharp.Objects.Matrix
open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Expecto
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects.ClCell

let context = Context.defaultContext

let clContext = context.ClContext
let processor = context.Queue

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

let deviceLeftMatrix = leftMatrix.ToDevice clContext
let deviceRightMatrix = rightMatrix.ToDevice clContext

let requiredRawsLengths () =
    let getRequiredRawsLengths =
        Expand.processLeftMatrixColumnsAndRightMatrixRawPointers clContext Utils.defaultWorkGroupSize Expand.requiredRawsLengths

    getRequiredRawsLengths processor deviceLeftMatrix.Columns deviceRightMatrix.RowPointers

let requiredRowLengthTest =
    testCase "requiredRowLength"
    <| fun () ->
        let actual = requiredRawsLengths().ToHostAndFree processor

        "Results must be the same"
        |> Expect.equal actual [| 2; 3; 3; 3; 2; 2; 0; 3 |]

let globalLength =
    let prefixSumExclude =
        PrefixSum.standardExcludeInplace clContext Utils.defaultWorkGroupSize

    let requiredRawsLengths = requiredRawsLengths ()

    (prefixSumExclude processor requiredRawsLengths).ToHostAndFree processor

let globalLengthTest =
    testCase "global length test"
    <| fun () -> Expect.equal globalLength 18 "Results must be the same"

let getGlobalRightMatrixRawsStartPositions () =
    let prefixSumExclude =
        PrefixSum.standardExcludeInplace clContext Utils.defaultWorkGroupSize

    let requiredRawsLengths = requiredRawsLengths ()

    (prefixSumExclude processor requiredRawsLengths).Free processor

    requiredRawsLengths

let globalRightMatrixRawsStartPositionsTest =
    testCase "global right matrix raws start positions"
    <| fun () ->
        let result = (getGlobalRightMatrixRawsStartPositions ()).ToHostAndFree processor

        "Results must be the same"
        |> Expect.equal result [| 0; 2; 5; 8; 11; 13; 15; 15; |]

let getRequiredRightMatrixValuesPointers () =
    let getRequiredRightMatrixValuesPointers =
        Expand.processLeftMatrixColumnsAndRightMatrixRawPointers clContext Utils.defaultWorkGroupSize Expand.requiredRawPointers

    getRequiredRightMatrixValuesPointers processor deviceLeftMatrix.Columns deviceRightMatrix.RowPointers

let getRequiredRightMatrixValuesPointersTest =
    testCase "get required right matrix values pointers"
    <| fun () ->
        let result = (getRequiredRightMatrixValuesPointers ()).ToHostAndFree processor

        "Result must be the same"
        |> Expect.equal result [| 3; 5; 0; 5; 8; 3; 0; 0; |]

let getGlobalPositions () =
    let getGlobalPositions = Expand.getGlobalPositions clContext Utils.defaultWorkGroupSize

    getGlobalPositions processor globalLength (getGlobalRightMatrixRawsStartPositions ())

let getGlobalPositionsTest =
    testCase "getGlobalPositions test"
    <| fun () ->
        let result = (getGlobalPositions ()).ToHostAndFree processor

        "Result must be the same"
        |> Expect.equal result [| 1; 1; 2; 2; 2; 3; 3; 3; 4; 4; 4; 5; 5; 6; 6; 7; 7; 7; |]

let getRightMatrixValuesPointers () =
    let getRightMatrixValuesPointers =
        Expand.getRightMatrixPointers clContext Utils.defaultWorkGroupSize

    let globalPositions = getGlobalPositions ()
    let globalRightMatrixRawsStartPositions = getGlobalRightMatrixRawsStartPositions ()
    let requiredRightMatrixValuesPointers = getRequiredRightMatrixValuesPointers ()

    getRightMatrixValuesPointers processor globalLength globalPositions globalRightMatrixRawsStartPositions requiredRightMatrixValuesPointers

let rightMatrixValuesPointersTest =
    testCase "RightMatrixValuesPointers"
    <| fun () ->
        let result = (getRightMatrixValuesPointers ()).ToHostAndFree processor

        "Result must be the same"
        |> Expect.equal result [| 3; 4; 5; 6; 7; 0; 1; 2; 5; 6; 7; 8; 9; 3; 4; 0; 1; 2; |]
