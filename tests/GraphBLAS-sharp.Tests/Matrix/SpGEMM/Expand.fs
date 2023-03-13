module GraphBLAS.FSharp.Tests.Backend.Matrix.SpGEMM.Expand

open GraphBLAS.FSharp.Objects.Matrix
open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Expecto
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClContext

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
      ColumnIndices = [| 2; 3; 1; 3; 4; 2; 0; 1 |]
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

let processPosition () =
    let processPositions = Expand.processPositions clContext Utils.defaultWorkGroupSize

    processPositions processor deviceLeftMatrix deviceRightMatrix

let processPositionsTest =
    testCase "ProcessPositions test"
    <| fun () ->
        let globalMap, globalRightMatrixRowsPointers, requiredLeftMatrixValues, requiredRightMatrixRowPointers, resultRowPointers
                = processPosition ()

        "Global map must be the same"
        |> Expect.equal (globalMap.ToHostAndFree processor) [| 1; 1; 2; 2; 2; 3; 3; 3; 4; 4; 4; 5; 5; 6; 6; 7; 7; 7; |]

        "global right matrix rows pointers must be the same"
        |> Expect.equal (globalRightMatrixRowsPointers.ToHostAndFree processor) [| 0; 2; 5; 8; 11; 13; 15; |]

        "required left matrix values must be the same"
        |> Expect.equal (requiredLeftMatrixValues.ToHostAndFree processor) [| 2; 3; 8; 5; 4; 2; 7; |]

        "required right matrix row pointers"
        |> Expect.equal (requiredRightMatrixRowPointers.ToHostAndFree processor) [| 3; 5; 0; 5; 8; 3; 0; |]

        "row pointers must be the same"
        |> Expect.equal (resultRowPointers.ToHostAndFree processor) [| 0; 5; 5; 13; 15; 18 |]

let expandLeftMatrixValues () =
    let expandLeftMatrixValues = Expand.expandLeftMatrixValues clContext Utils.defaultWorkGroupSize

    let globalMap, globalRightMatrixRowsPointers, requiredLeftMatrixValues, requiredRightMatrixRowPointers, resultRowPointers
            = processPosition ()

    let result = expandLeftMatrixValues processor globalMap requiredLeftMatrixValues

    globalMap.Free processor
    globalRightMatrixRowsPointers.Free processor
    requiredLeftMatrixValues.Free processor
    requiredRightMatrixRowPointers.Free processor
    resultRowPointers.Free processor

    result

let expandLeftMatrixValuesTest =
    testCase "expandLeftMatrixValues test"
    <| fun () ->
        let expandedLeftMatrixValues = (expandLeftMatrixValues ()).ToHostAndFree processor

        "Expand left matrix values must be the same"
        |> Expect.equal expandedLeftMatrixValues [| 2; 2; 3; 3; 3; 8; 8; 8; 5; 5; 5; 4; 4; 2; 2; 7; 7; 7 |]

let expandGlobalRightMatrixPointers () =
    let expandRightMatrixValuesPointers =
        Expand.expandRightMatrixValuesIndices clContext Utils.defaultWorkGroupSize

    let globalMap, globalRightMatrixRowsPointers, requiredLeftMatrixValues, requiredRightMatrixRowPointers, resultRowPointers = processPosition ()

    let globalRightMatrixValuesPointers =
        expandRightMatrixValuesPointers processor globalRightMatrixRowsPointers requiredRightMatrixRowPointers globalMap

    globalMap.Free processor
    globalRightMatrixRowsPointers.Free processor
    requiredLeftMatrixValues.Free processor
    requiredRightMatrixRowPointers.Free processor
    resultRowPointers.Free processor

    globalRightMatrixValuesPointers

let extendGlobalRightMatrixPointersTest =
    testCase "expandRightMatrixRowPointers test "
    <| fun () ->
        let expandedRowPointers = (expandGlobalRightMatrixPointers ()).ToHostAndFree processor

        "row pointers must be the same"
        |> Expect.equal expandedRowPointers [| 3; 4; 5; 6; 7; 0; 1; 2; 5; 6; 7; 8; 9; 3; 4; 0; 1; 2; |]

let getRightMatrixValuesAndColumns () =
    let getRightMatrixColumnsAndValues =
        Expand.getRightMatrixColumnsAndValues clContext Utils.defaultWorkGroupSize

    let globalRightMatrixValuesPointers = expandGlobalRightMatrixPointers ()

    getRightMatrixColumnsAndValues processor globalRightMatrixValuesPointers deviceRightMatrix

let getRightMatrixValuesAndPointersTest =
    testCase "expandRightMatrixValuesAndColumns"
    <| fun () ->
        let extendedRightMatrixValues, extendedRightMatrixColumns = getRightMatrixValuesAndColumns ()

        "extendedRightMatrixValues must be the same"
        |> Expect.equal (extendedRightMatrixValues.ToHostAndFree processor) [| 2; 2; 5; 9; 1; 3; 4; 4; 5; 9; 1; 1; 8; 2; 2; 3; 4; 4; |]

        "extendedRightMatrixColumns must be the same"
        |> Expect.equal (extendedRightMatrixColumns.ToHostAndFree processor) [| 2; 5; 1; 5; 6; 1; 4; 6; 1; 5; 6; 4; 6; 2; 5; 1; 4; 6; |]

let multiplication () =
    let map2 = ClArray.map2 clContext Utils.defaultWorkGroupSize <@ (*) @>

    let expandedLeftMatrixValues = expandLeftMatrixValues ()

    let extendedRightMatrixValues, extendedRightMatrixColumns = getRightMatrixValuesAndColumns ()
    extendedRightMatrixColumns.Free processor

    let multiplicationResult  =
        map2 processor DeviceOnly expandedLeftMatrixValues extendedRightMatrixValues

    expandedLeftMatrixValues.Free processor
    extendedRightMatrixValues.Free processor

    multiplicationResult

let multiplicationTest =
    testCase "multiplication test" <| fun () ->
        let result = (multiplication ()).ToHostAndFree processor

        "Results must be the same"
        |> Expect.equal result [| 4; 4; 15; 27; 3; 24; 32; 32; 25; 45; 5; 4; 32; 4; 4; 21; 28; 28 |]

let runExtendTest =
    testCase "Expand.run test" <| fun () ->
        let run = Expand.run clContext Utils.defaultWorkGroupSize <@ (*) @>

        let multiplicationResult, extendedRightMatrixColumns, resultRowPointers =
            run processor deviceLeftMatrix deviceRightMatrix

        "Results must be the same"
        |> Expect.equal (multiplicationResult.ToHostAndFree processor) [| 4; 4; 15; 27; 3; 24; 32; 32; 25; 45; 5; 4; 32; 4; 4; 21; 28; 28 |]

        "extendedRightMatrixColumns must be the same"
        |> Expect.equal (extendedRightMatrixColumns.ToHostAndFree processor) [| 2; 5; 1; 5; 6; 1; 4; 6; 1; 5; 6; 4; 6; 2; 5; 1; 4; 6; |]

        "row pointers must be the same"
        |> Expect.equal (resultRowPointers.ToHostAndFree processor) [| 0; 5; 5; 13; 15; 18 |]

