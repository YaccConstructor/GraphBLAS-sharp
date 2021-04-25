module Mxm

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

let logger = Log.create "MxmTests"

type OperationCase =
    {
        ClContext: OpenCLEvaluationContext
        LeftMatrixCase: MatrixType
        RightMatrixCase: MatrixType
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<MatrixType> |> Seq.map box
        Utils.listOfUnionCases<MatrixType> |> Seq.map box
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

type PairOfMatricesOfCompatibleSize() =
    static member IntType() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0 Arb.generate<int>
        |> Arb.fromGen

    static member FloatType() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
        |> Arb.fromGen

    static member SByteType() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0y Arb.generate<sbyte>
        |> Arb.fromGen

    static member ByteType() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0uy Arb.generate<byte>
        |> Arb.fromGen

    static member Int16Type() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0s Arb.generate<int16>
        |> Arb.fromGen

    static member UInt16Type() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0us Arb.generate<uint16>
        |> Arb.fromGen

    static member BoolType() =
        Generators.pairOfMatricesOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator false Arb.generate<bool>
        |> Arb.fromGen

let checkCorrectnessGeneric<'a when 'a : struct>
    (isEqual: 'a -> 'a -> bool)
    (semiring: ISemiring<'a>)
    (case: OperationCase)
    (leftMatrix: 'a[,], rightMatrix: 'a[,]) =

    let isZero = isEqual semiring.Zero

    let createMatrixFromArray2D matrixFormat array isZero =
        match matrixFormat with
        | CSR -> MatrixCSR <| CSRMatrix.FromArray2D(array, isZero)
        | COO -> MatrixCOO <| COOMatrix.FromArray2D(array, isZero)

    let mxmNaive (leftMatrix: 'a[,]) (rightMatrix: 'a[,]) =
        let resultRowCount = Array2D.length1 leftMatrix
        let resultColCount = Array2D.length2 rightMatrix
        let resultMatrix = Array2D.zeroCreate<'a> resultRowCount resultColCount

        for idx = 0 to resultRowCount * resultColCount - 1 do
            let i = idx / resultColCount
            let j = idx % resultColCount
            let leftRow = leftMatrix.[i, *]
            let rightCol = rightMatrix.[*, j]

            resultMatrix.[i, j] <-
                leftRow
                |> Array.mapi (fun i v -> semiring.Times.Eval v rightCol.[i])
                |> Array.reduce (fun x y -> semiring.Times.Eval x y)

        resultMatrix
        |> Seq.cast<'a>
        |> Seq.mapi
            (fun idx v ->
                let i = idx / Array2D.length2 leftMatrix
                let j = idx % Array2D.length2 leftMatrix

                (i, j, v)
            )
        |> Seq.filter (fun (_, _, v) -> not <| isZero v)
        |> Array.ofSeq
        |> Array.unzip3
        |> fun (rows, cols, vals) ->
            {
                RowIndices = rows
                ColumnIndices = cols
                Values = vals
            }

    let mxmGB (leftMatrix: 'a[,]) (rightMatrix: 'a[,]) =
        try
            let left = createMatrixFromArray2D case.LeftMatrixCase leftMatrix isZero
            let right = createMatrixFromArray2D case.RightMatrixCase rightMatrix isZero

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

    let expected = mxmNaive leftMatrix rightMatrix
    let actual = mxmGB leftMatrix rightMatrix

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

let config =
    { FsCheckConfig.defaultConfig with
        arbitrary = [typeof<PairOfMatricesOfCompatibleSize>]
        maxTest = 10
        startSize = 0
        // endSize = 1_000_000
    }

let testFixtures case = [
    let getTestName datatype =
        sprintf "Correctness on %s, %A, %A, %A, %O"
            datatype
            case.LeftMatrixCase
            case.RightMatrixCase
            case.MaskCase
            case.ClContext

    case
    |> checkCorrectnessGeneric<int> (=) AddMult.int
    |> testPropertyWithConfig config (getTestName "int")

    case
    |> checkCorrectnessGeneric<float> (fun x y -> abs (x - y) < Accuracy.medium.absolute) AddMult.float
    |> testPropertyWithConfig config (getTestName "float")

    case
    |> checkCorrectnessGeneric<sbyte> (=) AddMult.sbyte
    |> ptestPropertyWithConfig config (getTestName "sbyte")

    case
    |> checkCorrectnessGeneric<byte> (=) AddMult.byte
    |> testPropertyWithConfig config (getTestName "byte")

    case
    |> checkCorrectnessGeneric<int16> (=) AddMult.int16
    |> testPropertyWithConfig config (getTestName "int16")

    case
    |> checkCorrectnessGeneric<uint16> (=) AddMult.uint16
    |> testPropertyWithConfig config (getTestName "uint16")

    case
    |> checkCorrectnessGeneric<bool> (=) AnyAll.bool
    |> testPropertyWithConfig config (getTestName "bool")
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
    |> testList "Mxm tests"
