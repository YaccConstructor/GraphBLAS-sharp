module EWiseAdd

open Expecto
open FsCheck
open GraphBLAS.FSharp
open MathNet.Numerics
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Tests
open System
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open BackendState

type OperationParameter =
    | MatrixFormatParam of MatrixBackendFormat
    | MaskTypeParam of MaskType

type OperationCase = {
    MatrixCase: MatrixBackendFormat
    MaskCase: MaskType
}

let testCases =
    [
        Utils.listOfUnionCases<MatrixBackendFormat> |> List.map MatrixFormatParam
        Utils.listOfUnionCases<MaskType> |> List.map MaskTypeParam
    ]
    |> Utils.cartesian
    |> List.map
        (fun list ->
            let (MatrixFormatParam marixFormat) = list.[0]
            let (MaskTypeParam maskType) = list.[1]
            {
                MatrixCase = marixFormat
                MaskCase = maskType
            }
        )

type PairOfSparseMatricesOfEqualSize =
    static member IntType() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                Arb.generate<int>
                Gen.constant 0
            ]
        ) |> Arb.fromGen

    static member FloatType() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
                Gen.constant 0.
            ]
        ) |> Arb.fromGen

    static member SByteType() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                Arb.generate<sbyte>
                Gen.constant 0y
            ]
        ) |> Arb.fromGen

    static member ByteType() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                Arb.generate<byte>
                Gen.constant 0uy
            ]
        ) |> Arb.fromGen

    static member Int16Type() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                Arb.generate<int16>
                Gen.constant 0s
            ]
        ) |> Arb.fromGen

    static member UInt16Type() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                Arb.generate<uint16>
                Gen.constant 0us
            ]
        ) |> Arb.fromGen

    static member BoolType() =
        Generators.pairOfMatricesOfEqualSizeGenerator (
            Gen.oneof [
                Arb.generate<bool>
                Gen.constant false
            ]
        ) |> Arb.fromGen

let createMatrix<'a when 'a : struct and 'a : equality> matrixFormat args =
    match matrixFormat with
    | CSR ->
        Activator.CreateInstanceGeneric<CSRMatrix<_>>(
            Array.singleton typeof<'a>, args
        )
        |> unbox<CSRMatrix<'a>>
        :> Matrix<'a>
    | COO ->
        Activator.CreateInstanceGeneric<COOMatrix<_>>(
            Array.singleton typeof<'a>, args
        )
        |> unbox<COOMatrix<'a>>
        :> Matrix<'a>

let logger = Log.create "Sample"

let checkCorrectnessGeneric<'a when 'a : struct and 'a : equality>
    (sum: 'a -> 'a -> 'a)
    (diff: 'a -> 'a -> 'a)
    (isZero: 'a -> bool)
    (semiring: Semiring<'a>)
    (case: OperationCase)
    (matrixA: 'a[,], matrixB: 'a[,]) =

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
            let left = createMatrix<'a> case.MatrixCase [|matrixA; isZero|]
            let right = createMatrix<'a> case.MatrixCase [|matrixB; isZero|]

            logger.debug (
                eventX "Left matrix is \n{matrix}"
                >> setField "matrix" left
            )

            logger.debug (
                eventX "Right matrix is \n{matrix}"
                >> setField "matrix" right
            )

            opencl {
                let! result = left.EWiseAdd right None semiring
                let! tuples = result.GetTuples()
                return! tuples.ToHost()
            }
            |> oclContext.RunSync

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
        startSize = 0
        maxTest = 10
}

// https://docs.microsoft.com/ru-ru/dotnet/csharp/language-reference/language-specification/types#value-types
let testFixtures case = [
    case
    |> checkCorrectnessGeneric<int> (+) (-) ((=) 0) AddMult.int
    |> testPropertyWithConfig config (sprintf "Correctness on int, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<float> (+) (-) (fun x -> abs x < Accuracy.medium.absolute) AddMult.float
    |> testPropertyWithConfig config (sprintf "Correctness on float, %A, %A" case.MatrixCase case.MaskCase)

    // case
    // |> checkCorrectnessGeneric<sbyte> (+) (-) ((=) 0y) AddMult.sbyte
    // |> testPropertyWithConfig config (sprintf "Correctness on sbyte, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<byte> (+) (-) ((=) 0uy) AddMult.byte
    |> testPropertyWithConfig config (sprintf "Correctness on byte, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<int16> (+) (-) ((=) 0s) AddMult.int16
    |> testPropertyWithConfig config (sprintf "Correctness on int16, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<uint16> (+) (-) ((=) 0us) AddMult.uint16
    |> testPropertyWithConfig config (sprintf "Correctness on uint16, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<bool> (||) (<>) not AnyAll.bool
    |> testPropertyWithConfig config (sprintf "Correctness on bool, %A, %A" case.MatrixCase case.MaskCase)
]

let tests =
    testCases
    |> List.filter (fun case -> case.MatrixCase = COO && case.MaskCase = NoMask)
    |> List.collect testFixtures
    |> testList "EWiseAdd tests"
