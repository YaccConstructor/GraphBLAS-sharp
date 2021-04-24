module Vxm

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
        VectorCase: VectorType
        MatrixCase: MatrixType
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<VectorType> |> Seq.map box
        Utils.listOfUnionCases<MatrixType> |> Seq.map box
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

type PairOfSparseVectorAndMatrixOfCompatibleSize() =
    static member IntType() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0 Arb.generate<int>
        |> Arb.fromGen

    static member FloatType() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
        |> Arb.fromGen

    static member SByteType() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0y Arb.generate<sbyte>
        |> Arb.fromGen

    static member ByteType() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0uy Arb.generate<byte>
        |> Arb.fromGen

    static member Int16Type() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0s Arb.generate<int16>
        |> Arb.fromGen

    static member UInt16Type() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator 0us Arb.generate<uint16>
        |> Arb.fromGen

    static member BoolType() =
        Generators.pairOfVectorAndMatrixOfCompatibleSizeGenerator
        |> Generators.genericSparseGenerator false Arb.generate<bool>
        |> Arb.fromGen

// let checkCorrectnessGeneric<'a when 'a : struct>
//     (oclContext: OpenCLEvaluationContext)
//     (sum: 'a -> 'a -> 'a)
//     (diff: 'a -> 'a -> 'a)
//     (isZero: 'a -> bool)
//     (semiring: ISemiring<'a>)
//     (case: OperationCase)
//     (matrixA: 'a[,], matrixB: 'a[,], mask: bool[,]) =

//     let createMatrixFromArray2D array isZero =
//         match case.MatrixCase with
//         | CSR -> failwith "Not implemented"
//         | COO -> MatrixCOO <| COOMatrix.FromArray2D(array, isZero)

//     let createVectorFromArray array isZero =
//         match case.VectorCase with
//         | VectorType.COO -> VectorCOO <| COOVector.FromArray(array, isZero)

//     let vxmNaive (vector: 'a[]) (matrix: 'a[,]) =
//         let left = matrixA |> Seq.cast<'a>
//         let right = matrixB |> Seq.cast<'a>

//         (left, right)
//         ||> Seq.mapi2
//             (fun idx x y ->
//                 let i = idx / Array2D.length2 matrixA
//                 let j = idx % Array2D.length2 matrixA

//                 if isZero x && isZero y then None
//                 else Some (i, j, sum x y)
//             )
//         |> Seq.choose id
//         |> Array.ofSeq
//         |> Array.unzip3
//         |> fun (rows, cols, vals) ->
//             {
//                 RowIndices = rows
//                 ColumnIndices = cols
//                 Values = vals
//             }

//     let eWiseAddGB (matrixA: 'a[,]) (matrixB: 'a[,]) =
//         try
//             let left = createMatrixFromArray2D matrixA isZero
//             let right = createMatrixFromArray2D matrixB isZero

//             logger.debug (
//                 eventX "Left matrix is \n{matrix}"
//                 >> setField "matrix" left
//             )

//             logger.debug (
//                 eventX "Right matrix is \n{matrix}"
//                 >> setField "matrix" right
//             )

//             graphblas {
//                 let! result = Vector.vxm semiring left right
//                 let! tuples = Matrix.tuples result
//                 do! MatrixTuples.synchronize tuples
//                 return tuples
//             }
//             |> EvalGB.withClContext oclContext
//             |> EvalGB.runSync

//         finally
//             oclContext.Provider.CloseAllBuffers()

//     let expected = eWiseAddNaive matrixA matrixB
//     let actual = eWiseAddGB matrixA matrixB

//     logger.debug (
//         eventX "Expected result is {matrix}"
//         >> setField "matrix" (sprintf "%A" expected.Values)
//     )

//     logger.debug (
//         eventX "Actual result is {matrix}"
//         >> setField "matrix" (sprintf "%A" actual.Values)
//     )

//     let actualIndices = Seq.zip actual.RowIndices actual.ColumnIndices
//     let expectedIndices = Seq.zip expected.RowIndices expected.ColumnIndices

//     "Indices of expected and result matrix must be the same"
//     |> Expect.sequenceEqual actualIndices expectedIndices

//     let difference =
//         (expected.Values, actual.Values)
//         ||> Seq.map2 diff

//     "Length of expected and result values should be equal"
//     |> Expect.hasLength actual.Values (Seq.length expected.Values)

//     "There should be no difference between expected and received values"
//     |> Expect.all difference isZero

let config =
    { FsCheckConfig.defaultConfig with
        arbitrary = [typeof<PairOfSparseVectorAndMatrixOfCompatibleSize>]
        maxTest = 10
        startSize = 0
        // endSize = 1_000_000
    }

let testFixtures case = []

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device
            let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

            deviceType = DeviceType.Cpu &&
            case.VectorCase = VectorType.COO &&
            case.MatrixCase = CSR &&
            case.MaskCase <> Complemented
        )
    |> List.collect testFixtures
    |> testList "Vxm tests"
