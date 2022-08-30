module Matrix.GetTuples

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open OpenCL.Net

let logger = Log.create "Matrix.GetTuples.Tests"

type OperationCase = { ClContext: ClContext; MatrixCase: MatrixFromat }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<MatrixFromat> |> Seq.map box
    ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map (fun list -> { ClContext = unbox list.[0]; MatrixCase = unbox list.[1] })

let correctnessGenericTest<'a when 'a: struct>
    (isEqual: 'a -> 'a -> bool)
    (zero: 'a)
    (case: OperationCase)
    (matrix: 'a [,])
    =

    let isZero = isEqual zero

    let expected =
        matrix
        |> Seq.cast<'a>
        |> Seq.mapi (fun idx v ->
            let i = idx / Array2D.length2 matrix
            let j = idx % Array2D.length2 matrix

            (i, j, v)
        )
        |> Seq.filter (fun (_, _, v) -> (not << isZero) v)
        |> Array.ofSeq
        |> Array.unzip3
        |> fun (rows, cols, vals) -> { RowIndices = rows; ColumnIndices = cols; Values = vals }

    let actual =
        try
            let matrix = Utils.createMatrixFromArray2D case.MatrixCase matrix isZero

            logger.debug (eventX "Matrix is \n{matrix}" >> setField "matrix" matrix)

            graphblas {
                let! tuples = Matrix.tuples matrix
                do! MatrixTuples.synchronize tuples
                return tuples
            }
            |> EvalGB.withClContext case.ClContext
            |> EvalGB.runSync

        finally
            // TODO fix me
            ()
    //case.ClContext.Provider.CloseAllBuffers()

    logger.debug (
        eventX "Expected result is {expected}"
        >> setField "expected" (sprintf "%A" expected.Values)
    )

    logger.debug (
        eventX "Actual result is {actual}"
        >> setField "actual" (sprintf "%A" actual.Values)
    )

    let actualIndices = Seq.zip actual.RowIndices actual.ColumnIndices

    let expectedIndices = Seq.zip expected.RowIndices expected.ColumnIndices

    "Indices of expected and result matrix must be the same"
    |> Expect.sequenceEqual actualIndices expectedIndices

    let equality = (expected.Values, actual.Values) ||> Seq.map2 isEqual

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.allEqual equality true

let testFixtures case =
    [
        let config = Utils.defaultConfig

        let getCorrectnessTestName datatype = sprintf "Correctness on %s, %A" datatype case

        case
        |> correctnessGenericTest<int> (=) 0
        |> testPropertyWithConfig config (getCorrectnessTestName "int")

        case
        |> correctnessGenericTest<float> (fun x y -> abs (x - y) < Accuracy.medium.absolute) 0.
        |> testPropertyWithConfig config (getCorrectnessTestName "float")

        case
        |> correctnessGenericTest<int16> (=) 0s
        |> testPropertyWithConfig config (getCorrectnessTestName "int16")

        case
        |> correctnessGenericTest<uint16> (=) 0us
        |> testPropertyWithConfig config (getCorrectnessTestName "uint16")

        case
        |> correctnessGenericTest<bool> (=) false
        |> ptestPropertyWithConfig config (getCorrectnessTestName "bool")
    ]

let tests =
    testCases
    |> List.filter (fun case ->
        let mutable e = ErrorCode.Unknown
        let device = case.ClContext.Device

        let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

        deviceType = DeviceType.Cpu && case.MatrixCase = CSR
    )
    |> List.collect testFixtures
    |> testList "Matrix.tuples tests"
