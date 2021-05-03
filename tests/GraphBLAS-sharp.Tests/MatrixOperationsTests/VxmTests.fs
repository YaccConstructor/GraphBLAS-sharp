module Matrix.Vxm

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

let logger = Log.create "VxmTests"

type OperationCase =
    {
        ClContext: OpenCLEvaluationContext
        VectorCase: VectorFormat
        MatrixCase: MatrixFromat
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<VectorFormat> |> Seq.map box
        Utils.listOfUnionCases<MatrixFromat> |> Seq.map box
        Utils.listOfUnionCases<MaskType> |> Seq.map box
    ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map
        (fun list -> {
            ClContext = unbox list.[0]
            VectorCase = unbox list.[1]
            MatrixCase = unbox list.[2]
            MaskCase = unbox list.[3]
        })

let checkCorrectnessGeneric<'a when 'a : struct>
    (semiring: ISemiring<'a>)
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (vector: 'a[], matrix: 'a[,], mask: bool[]) =

    let isZero = isEqual semiring.Zero

    let expected =
        let resultSize = Array2D.length2 matrix
        let resultVector = Array.zeroCreate<'a> resultSize

        for i = 0 to resultSize - 1 do
            let col = matrix.[*, i]
            resultVector.[i] <-
                vector
                |> Array.mapi (fun i v -> semiring.Times.Invoke v col.[i])
                |> Array.reduce (fun x y -> semiring.Plus.Invoke x y)

        resultVector
        |> Seq.cast<'a>
        |> Seq.mapi (fun i v -> (i, v))
        |> Seq.filter
            (fun (i, v) ->
                not (isZero v) &&
                match case.MaskCase with
                | NoMask -> true
                | Regular -> mask.[i]
                | Complemented -> not mask.[i]
            )
        |> Array.ofSeq
        |> Array.unzip
        |> fun (cols, vals) ->
            {
                Indices = cols
                Values = vals
            }

    let actual =
        try
            let vector = Utils.createVectorFromArray case.VectorCase vector isZero
            let matrix = Utils.createMatrixFromArray2D case.MatrixCase matrix isZero
            let mask = Utils.createVectorFromArray VectorFormat.COO mask not

            logger.debug (
                eventX "Vector is \n{vector}"
                >> setField "vector" vector
            )

            logger.debug (
                eventX "Matrix is \n{matrix}"
                >> setField "matrix" matrix
            )

            graphblas {
                let! result =
                    match case.MaskCase with
                    | NoMask -> Matrix.vxm semiring vector matrix
                    | Regular ->
                        Vector.mask mask
                        >>= fun mask -> Matrix.vxmWithMask semiring mask vector matrix
                    | Complemented ->
                        Vector.complemented mask
                        >>= fun mask -> Matrix.vxmWithMask semiring mask vector matrix

                let! tuples = Vector.tuples result
                do! VectorTuples.synchronize tuples
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

    let actualIndices = actual.Indices
    let expectedIndices = expected.Indices

    "Indices of expected and result vector must be the same"
    |> Expect.sequenceEqual actualIndices expectedIndices

    let equality =
        (expected.Values, actual.Values)
        ||> Seq.map2 isEqual

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.allEqual equality true

let testFixtures case = [
    let getTestName datatype =
        sprintf "Correctness on %s, %A, %A, %A, %O"
            datatype
            case.VectorCase
            case.MatrixCase
            case.MaskCase
            case.ClContext

    case
    |> checkCorrectnessGeneric<int> AddMult.int (=)
    |> ftestPropertyWithConfig Utils.defaultConfig (getTestName "int")

    case
    |> checkCorrectnessGeneric<float> AddMult.float (fun x y -> abs (x - y) < Accuracy.medium.absolute)
    |> ftestPropertyWithConfig Utils.defaultConfig (getTestName "float")

    case
    |> checkCorrectnessGeneric<sbyte> AddMult.sbyte (=)
    |> ptestPropertyWithConfig Utils.defaultConfig (getTestName "sbyte")

    case
    |> checkCorrectnessGeneric<byte> AddMult.byte (=)
    |> ptestPropertyWithConfig Utils.defaultConfig (getTestName "byte")

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
            case.VectorCase = VectorFormat.COO &&
            case.MatrixCase = CSR &&
            case.MaskCase = NoMask
        )
    |> List.collect testFixtures
    |> testList "Vxm tests"
