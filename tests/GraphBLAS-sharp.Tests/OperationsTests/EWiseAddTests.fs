module EWiseAdd

open Expecto
open FsCheck
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open BackendState
open GraphBLAS.FSharp.Helpers

let logger = Log.create "EWiseAddTests"

type OperationCase =
    {
        MatrixCase: MatrixBackendFormat
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.listOfUnionCases<MatrixBackendFormat> |> List.map box
        Utils.listOfUnionCases<MaskType> |> List.map box
    ]
    |> Utils.cartesian
    |> List.map ^fun list ->
        {
            MatrixCase = unbox list.[0]
            MaskCase = unbox list.[1]
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
            |> EvalGB.runWithClContext oclContext

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
        maxTest = 50
        startSize = 0
        endSize = 1_000_000
}

// https://docs.microsoft.com/ru-ru/dotnet/csharp/language-reference/language-specification/types#value-types
let testFixtures case = [
    case
    |> checkCorrectnessGeneric<int> (+) (-) ((=) 0) AddMult.int
    |> testPropertyWithConfig config (sprintf "Correctness on int, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<float> (+) (-) (fun x -> abs x < Accuracy.medium.absolute) AddMult.float
    |> testPropertyWithConfig config (sprintf "Correctness on float, %A, %A" case.MatrixCase case.MaskCase)

    case
    |> checkCorrectnessGeneric<sbyte> (+) (-) ((=) 0y) AddMult.sbyte
    |> testPropertyWithConfig config (sprintf "Correctness on sbyte, %A, %A" case.MatrixCase case.MaskCase)

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
