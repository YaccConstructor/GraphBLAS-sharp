module Matrix.EWiseAdd

open Expecto
open FsCheck
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net

let logger = Log.create "EWiseAddTests"

type OperationCase =
    {
        ClContext: OpenCLEvaluationContext
        MatrixCase: MatrixFromat
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<MatrixFromat> |> Seq.map box
        Utils.listOfUnionCases<MaskType> |> Seq.map box
    ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map
        (fun list -> {
            ClContext = unbox list.[0]
            MatrixCase = unbox list.[1]
            MaskCase = unbox list.[2]
        })

let checkCorrectnessGeneric<'a when 'a : struct>
    (monoid: IMonoid<'a>)
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (leftMatrix: 'a[,], rightMatrix: 'a[,]) =

    let isZero = isEqual monoid.Zero

    let createMatrixFromArray2D matrixFormat array =
        match matrixFormat with
        | CSR -> failwith "Not implemented"
        | COO -> MatrixCOO <| COOMatrix.FromArray2D(array, isZero)

    let expected =
        let left = leftMatrix |> Seq.cast<'a>
        let right = rightMatrix |> Seq.cast<'a>

        (left, right)
        ||> Seq.mapi2
            (fun idx x y ->
                let i = idx / Array2D.length2 leftMatrix
                let j = idx % Array2D.length2 leftMatrix

                if isZero x && isZero y then None
                else Some (i, j, monoid.Plus.Invoke x y)
            )
        |> Seq.choose id
        |> Array.ofSeq
        |> Array.unzip3
        |> fun (rows, cols, vals) ->
            {
                RowIndices = rows
                ColumnIndices = cols
                Values = vals
            }

    let actual =
        try
            let left = createMatrixFromArray2D case.MatrixCase leftMatrix
            let right = createMatrixFromArray2D case.MatrixCase rightMatrix

            logger.debug (
                eventX "Left matrix is \n{matrix}"
                >> setField "matrix" left
            )

            logger.debug (
                eventX "Right matrix is \n{matrix}"
                >> setField "matrix" right
            )

            graphblas {
                let! result = Matrix.eWiseAdd monoid left right
                let! tuples = Matrix.tuples result
                do! MatrixTuples.synchronize tuples
                return tuples
            }
            |> EvalGB.withClContext case.ClContext
            |> EvalGB.runSync

        finally
            case.ClContext.Provider.CloseAllBuffers()

    logger.debug (
        eventX "Expected result is {matrix}"
        >> setField "matrix" (sprintf "%A" expected.Values)
    )

    logger.debug (
        eventX "Actual result is {matrix}"
        >> setField "matrix" (sprintf "%A" actual.Values)
    )

    let actualIndices = Seq.zip actual.RowIndices actual.ColumnIndices
    let expectedIndices = Seq.zip expected.RowIndices expected.ColumnIndices

    "Indices of expected and result matrix must be the same"
    |> Expect.sequenceEqual actualIndices expectedIndices

    let equality =
        (expected.Values, actual.Values)
        ||> Seq.map2 isEqual

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.allEqual equality true

// https://docs.microsoft.com/ru-ru/dotnet/csharp/language-reference/language-specification/types#value-types
let testFixtures case = [
    let getTestName datatype = sprintf "Correctness on %s, %A, %A" datatype case.MatrixCase case.MaskCase

    case
    |> checkCorrectnessGeneric<int> AddMult.int (=)
    |> testPropertyWithConfig Utils.defaultConfig (getTestName "int")

    case
    |> checkCorrectnessGeneric<float> AddMult.float (fun x y -> abs (x - y) < Accuracy.medium.absolute)
    |> testPropertyWithConfig Utils.defaultConfig (getTestName "float")

    case
    |> checkCorrectnessGeneric<sbyte> AddMult.sbyte (=)
    |> ptestPropertyWithConfig Utils.defaultConfig (getTestName "sbyte")

    case
    |> checkCorrectnessGeneric<byte> AddMult.byte (=)
    |> testPropertyWithConfig Utils.defaultConfig (getTestName "byte")

    case
    |> checkCorrectnessGeneric<int16> AddMult.int16 (=)
    |> testPropertyWithConfig Utils.defaultConfig (getTestName "int16")

    case
    |> checkCorrectnessGeneric<uint16> AddMult.uint16 (=)
    |> testPropertyWithConfig Utils.defaultConfig (getTestName "uint16")

    case
    |> checkCorrectnessGeneric<bool> AnyAll.bool (=)
    |> testPropertyWithConfig Utils.defaultConfig (getTestName "bool")
]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device
            let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

            deviceType = DeviceType.Cpu &&
            case.MatrixCase = COO &&
            case.MaskCase = NoMask
        )
    |> List.collect testFixtures
    |> testList "EWiseAdd tests"
