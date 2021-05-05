module Matrix.Mxm

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

let logger = Log.create "Matrix.Mxm.Tests"

type OperationCase =
    {
        ClContext: OpenCLEvaluationContext
        LeftMatrixCase: MatrixFromat
        RightMatrixCase: MatrixFromat
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<MatrixFromat> |> Seq.map box
        Utils.listOfUnionCases<MatrixFromat> |> Seq.map box
        Utils.listOfUnionCases<MaskType> |> Seq.map box
    ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map
        (fun list -> {
            ClContext = unbox list.[0]
            LeftMatrixCase = unbox list.[1]
            RightMatrixCase = unbox list.[2]
            MaskCase = unbox list.[3]
        })

let correctnessGenericTest<'a when 'a : struct>
    (semiring: ISemiring<'a>)
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (leftMatrix: 'a[,], rightMatrix: 'a[,]) =

    let isZero = isEqual semiring.Zero

    let expected =
        let resultRowCount = Array2D.length1 leftMatrix
        let resultColCount = Array2D.length2 rightMatrix
        let resultMatrix = Array2D.zeroCreate<'a> resultRowCount resultColCount

        let plus = semiring.Plus.Invoke
        let times = semiring.Times.Invoke

        for idx = 0 to resultRowCount * resultColCount - 1 do
            let i = idx / resultColCount
            let j = idx % resultColCount
            let leftRow = leftMatrix.[i, *]
            let rightCol = rightMatrix.[*, j]

            resultMatrix.[i, j] <-
                leftRow
                |> Array.mapi (fun i v -> times v rightCol.[i])
                |> Array.reduce (fun x y -> plus x y)

        resultMatrix
        |> Seq.cast<'a>
        |> Seq.mapi
            (fun idx v ->
                let i = idx / Array2D.length2 leftMatrix
                let j = idx % Array2D.length2 leftMatrix

                (i, j, v)
            )
        |> Seq.filter (fun (_, _, v) -> (not << isZero) v)
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
            let left = Utils.createMatrixFromArray2D case.LeftMatrixCase leftMatrix isZero
            let right = Utils.createMatrixFromArray2D case.RightMatrixCase rightMatrix isZero

            logger.debug (
                eventX "Left matrix is \n{matrix}"
                >> setField "matrix" left
            )

            logger.debug (
                eventX "Right matrix is \n{matrix}"
                >> setField "matrix" right
            )

            graphblas {
                let! result = Matrix.mxm semiring left right
                let! tuples = Matrix.tuples result
                do! MatrixTuples.synchronize tuples
                return tuples
            }
            |> EvalGB.withClContext case.ClContext
            |> EvalGB.runSync

        finally
            case.ClContext.Provider.CloseAllBuffers()

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

    let equality =
        (expected.Values, actual.Values)
        ||> Seq.map2 isEqual

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.allEqual equality true

let testFixtures case = [
    let config = Utils.defaultConfig
    let getCorrectnessTestName datatype = sprintf "Correctness on %s, %A" datatype case

    case
    |> correctnessGenericTest<int> AddMult.int (=)
    |> testPropertyWithConfig config (getCorrectnessTestName "int")

    case
    |> correctnessGenericTest<float> AddMult.float (fun x y -> abs (x - y) < Accuracy.medium.absolute)
    |> testPropertyWithConfig config (getCorrectnessTestName "float")

    case
    |> correctnessGenericTest<int16> AddMult.int16 (=)
    |> testPropertyWithConfig config (getCorrectnessTestName "int16")

    case
    |> correctnessGenericTest<uint16> AddMult.uint16 (=)
    |> testPropertyWithConfig config (getCorrectnessTestName "uint16")

    case
    |> correctnessGenericTest<bool> AnyAll.bool (=)
    |> testPropertyWithConfig config (getCorrectnessTestName "bool")
]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device
            let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

            deviceType = DeviceType.Cpu &&
            case.LeftMatrixCase = CSR &&
            case.RightMatrixCase = CSR &&
            case.MaskCase = NoMask
        )
    |> List.collect testFixtures
    |> testList "Matrix.mxm tests"
