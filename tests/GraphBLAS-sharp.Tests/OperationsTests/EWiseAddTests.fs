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

type PairOfSparseMatrices =
    static member IntType() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            Arb.generate<int>
            0
            ((=) 0)

    static member FloatType() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
            0.
            (fun x -> abs x < Accuracy.medium.absolute)

    static member BoolType() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            Arb.generate<bool>
            false
            ((=) false)

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

let correctnessOnNumbers<'a when 'a : struct and 'a : equality>
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
        ||> Seq.map2
            (fun x y ->
                if isZero x && isZero y then None
                else Some <| sum x y
            )
        |> Seq.choose id

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
            |> (fun tuples -> tuples.Values)
            |> Seq.ofArray

        finally
            oclContext.Provider.CloseAllBuffers()

    let expected = eWiseAddNaive matrixA matrixB
    let actual = eWiseAddGB matrixA matrixB

    logger.debug (
        eventX "Expected result is {matrix}"
        >> setField "matrix" (sprintf "%A" <| List.ofSeq expected)
    )

    logger.debug (
        eventX "Actual result is {matrix}"
        >> setField "matrix" (sprintf "%A" <| List.ofSeq actual)
    )

    "Length of expected and result seq should be equal"
    |> Expect.hasLength actual (Seq.length expected)

    let difference =
        (expected, actual)
        ||> Seq.map2 diff

    "There should be no difference between expected and received values"
    |> Expect.all difference isZero

let correctnessOnBool (case: OperationCase) (matrixA: bool[,], matrixB: bool[,]) =
    let eWiseAddNaive (matrixA: bool[,]) (matrixB: bool[,]) =
        let left = matrixA |> Seq.cast<bool>
        let right = matrixB |> Seq.cast<bool>

        (left, right)
        ||> Seq.map2 (||)
        |> Seq.filter id

    let eWiseAddGB (matrixA: bool[,]) (matrixB: bool[,]) =
        try
            let left = createMatrix<bool> case.MatrixCase [|matrixA; not|]
            let right = createMatrix<bool> case.MatrixCase [|matrixB; not|]

            logger.debug (
                eventX "Left matrix is \n{matrix}"
                >> setField "matrix" left
            )

            logger.debug (
                eventX "Right matrix is \n{matrix}"
                >> setField "matrix" right
            )

            opencl {
                let! result = left.EWiseAdd right None AnyAll.bool
                let! tuples = result.GetTuples()
                return! tuples.ToHost()
            }
            |> oclContext.RunSync
            |> (fun tuples -> tuples.Values)
            |> Seq.ofArray

        finally
            oclContext.Provider.CloseAllBuffers()

    let expected = eWiseAddNaive matrixA matrixB
    let actual = eWiseAddGB matrixA matrixB

    logger.debug (
        eventX "Expected result is {matrix}"
        >> setField "matrix" (sprintf "%A" <| List.ofSeq expected)
    )

    logger.debug (
        eventX "Actual result is {matrix}"
        >> setField "matrix" (sprintf "%A" <| List.ofSeq actual)
    )

    "Length of expected and result seq should be equal"
    |> Expect.hasLength actual (Seq.length expected)

    let difference =
        (expected, actual)
        ||> Seq.map2 (<>)

    logger.debug (
        eventX "Difference result is {matrix}"
        >> setField "matrix" (sprintf "%A" <| List.ofSeq difference)
    )

    "There should be no difference between expected and received values"
    |> Expect.all difference not

let config = {
    FsCheckConfig.defaultConfig with
        arbitrary = [typeof<PairOfSparseMatrices>]
        startSize = 0
        maxTest = 10
}

// https://docs.microsoft.com/ru-ru/dotnet/csharp/language-reference/language-specification/types#value-types
let testFixtures case = [
    case
    |> correctnessOnNumbers<int> (+) (-) ((=) 0) AddMult.int
    |> testPropertyWithConfig config (sprintf "Correctness on int, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> correctnessOnNumbers<float> (+) (-) (fun x -> abs x < Accuracy.medium.absolute) AddMult.float
    |> testPropertyWithConfig config (sprintf "Correctness on float, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> correctnessOnBool
    |> testPropertyWithConfig config (sprintf "Correctness on bool, %A, %A" case.MatrixCase case.MaskCase)
]

let tests =
    testCases
    |> List.filter (fun case -> case.MatrixCase = COO && case.MaskCase = NoMask)
    |> List.collect testFixtures
    |> testList "EWiseAdd tests"
