module EWiseAdd

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
        MatrixCase: MatrixType
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<MatrixType> |> Seq.map box
        Utils.listOfUnionCases<MaskType> |> Seq.map box
    ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map ^fun list ->
        {
            ClContext = unbox list.[0]
            MatrixCase = unbox list.[1]
            MaskCase = unbox list.[2]
        }

type PairOfSparseMatricesOfEqualSize() =
    static let MaxSparcity = 100
    static let SparsityGen = Gen.choose (0, MaxSparcity)
    static let GenericSparseGen valueGenProvider =
        gen {
            let! sparsity = SparsityGen
            logger.debug (
                eventX "Sparcity is {sp} of {ms}"
                >> setField "sp" sparsity
                >> setField "ms" MaxSparcity
            )

            return! valueGenProvider sparsity
        }

    static member IntType() =
        fun sparsity ->
            Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.generate<int>)
                    (sparsity, Gen.constant 0)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

    static member FloatType() =
        fun sparsity ->
             Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
                    (sparsity, Gen.constant 0.)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

    static member SByteType() =
        fun sparsity ->
            Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.generate<sbyte>)
                    (sparsity, Gen.constant 0y)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

    static member ByteType() =
        fun sparsity ->
            Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.generate<byte>)
                    (sparsity, Gen.constant 0uy)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

    static member Int16Type() =
        fun sparsity ->
            Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.generate<int16>)
                    (sparsity, Gen.constant 0s)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

    static member UInt16Type() =
        fun sparsity ->
            Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.generate<uint16>)
                    (sparsity, Gen.constant 0us)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

    static member BoolType() =
        fun sparsity ->
            Generators.pairOfMatricesOfEqualSizeGenerator (
                Gen.frequency [
                    (MaxSparcity - sparsity, Arb.generate<bool>)
                    (sparsity, Gen.constant false)
                ]
            )
        |> GenericSparseGen
        |> Arb.fromGen

let checkCorrectnessGeneric<'a when 'a : struct and 'a : equality>
    (oclContext: OpenCLEvaluationContext)
    (sum: 'a -> 'a -> 'a)
    (diff: 'a -> 'a -> 'a)
    (isZero: 'a -> bool)
    (semiring: ISemiring<'a>)
    (case: OperationCase)
    (matrixA: 'a[,], matrixB: 'a[,]) =

    let createMatrixFromArray2D matrixFormat array isZero =
        match matrixFormat with
        | CSR -> failwith "Not implemented"
        | COO -> MatrixCOO <| COOMatrix.FromArray2D(array, isZero)

    let eWiseAddNaive (matrixA: 'a[,]) (matrixB: 'a[,]) =
        let left = matrixA |> Seq.cast<'a>
        let right = matrixB |> Seq.cast<'a>

        (left, right)
        ||> Seq.mapi2
            (fun idx x y ->
                let i = idx / Array2D.length2 matrixA
                let j = idx % Array2D.length2 matrixA

                if isZero x && isZero y then None
                else Some (i, j, sum x y)
            )
        |> Seq.choose id
        |> Array.ofSeq
        |> Array.unzip3
        |>
            (fun (rows, cols, vals) ->
                {
                    RowIndices = rows
                    ColumnIndices = cols
                    Values = vals
                }
            )

    let eWiseAddGB (matrixA: 'a[,]) (matrixB: 'a[,]) =
        try
            let left = createMatrixFromArray2D case.MatrixCase matrixA isZero
            let right = createMatrixFromArray2D case.MatrixCase matrixB isZero

            logger.debug (
                eventX "Left matrix is \n{matrix}"
                >> setField "matrix" left
            )

            logger.debug (
                eventX "Right matrix is \n{matrix}"
                >> setField "matrix" right
            )

            graphblas {
                let! result = Matrix.eWiseAdd semiring left right
                let! tuples = Matrix.tuples result
                do! MatrixTuples.synchronize tuples
                return tuples
            }
            |> EvalGB.withClContext oclContext
            |> EvalGB.runSync

        finally
            oclContext.Provider.CloseAllBuffers()

    let expected = eWiseAddNaive matrixA matrixB
    let actual = eWiseAddGB matrixA matrixB

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

    let difference =
        (expected.Values, actual.Values)
        ||> Seq.map2 diff

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.all difference isZero

let config = {
    FsCheckConfig.defaultConfig with
        arbitrary = [typeof<PairOfSparseMatricesOfEqualSize>]
        maxTest = 10
        startSize = 0
        // endSize = 1_000_000
}

// https://docs.microsoft.com/ru-ru/dotnet/csharp/language-reference/language-specification/types#value-types
let testFixtures case = [
    let getTestName datatype = sprintf "Correctness on %s, %A, %A" datatype case.MatrixCase case.MaskCase

    case
    |> checkCorrectnessGeneric<int> case.ClContext (+) (-) ((=) 0) AddMult.int
    |> testPropertyWithConfig config (getTestName "int")

    case
    |> checkCorrectnessGeneric<float> case.ClContext (+) (-) (fun x -> abs x < Accuracy.medium.absolute) AddMult.float
    |> testPropertyWithConfig config (getTestName "float")

    case
    |> checkCorrectnessGeneric<sbyte> case.ClContext (+) (-) ((=) 0y) AddMult.sbyte
    |> ptestPropertyWithConfig config (getTestName "sbyte")

    case
    |> checkCorrectnessGeneric<byte> case.ClContext (+) (-) ((=) 0uy) AddMult.byte
    |> testPropertyWithConfig config (getTestName "byte")

    case
    |> checkCorrectnessGeneric<int16> case.ClContext (+) (-) ((=) 0s) AddMult.int16
    |> testPropertyWithConfig config (getTestName "int16")

    case
    |> checkCorrectnessGeneric<uint16> case.ClContext (+) (-) ((=) 0us) AddMult.uint16
    |> testPropertyWithConfig config (getTestName "uint16")

    case
    |> checkCorrectnessGeneric<bool> case.ClContext (||) (<>) not AnyAll.bool
    |> testPropertyWithConfig config (getTestName "bool")

    case
    |> checkCorrectnessGeneric<bool> case.ClContext (||) (<>) not AnyAll.bool
    |> testPropertyWithConfigStdGen
        (355610228, 296870493)
        { FsCheckConfig.defaultConfig with
            arbitrary = [typeof<PairOfSparseMatricesOfEqualSize>]
            maxTest = 10
            startSize = 0
        }
        "Correctness on both empty matrices"
]

let tests =
    testCases
    |> List.filter (fun case -> case.MatrixCase = COO && case.MaskCase = NoMask)
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device
            // let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
            let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()
            deviceType = DeviceType.Cpu
        )
    |> List.collect testFixtures
    |> testList "EWiseAdd tests"
