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

// let checkCorrectnessGeneric<'a when 'a : struct>
//     (oclContext: OpenCLEvaluationContext)
//     (sum: 'a -> 'a -> 'a)
//     (diff: 'a -> 'a -> 'a)
//     (isZero: 'a -> bool)
//     (semiring: ISemiring<'a>)
//     (case: OperationCase)
//     (matrixA: 'a[,], matrixB: 'a[,]) =

//     let createMatrixFromArray2D matrixFormat array isZero =
//         match matrixFormat with
//         | CSR -> failwith "Not implemented"
//         | COO -> MatrixCOO <| COOMatrix.FromArray2D(array, isZero)

//     let mxmNaive (matrixA: 'a[,]) (matrixB: 'a[,]) =
//         let output = Array2D.zeroCreate<'a> (Array2D.length1 matrixA) (Array2D.length2 matrixB)

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

//     let mxmGB (matrixA: 'a[,]) (matrixB: 'a[,]) =
//         try
//             let left = createMatrixFromArray2D case.LeftMatrixCase matrixA isZero
//             let right = createMatrixFromArray2D case.RightMatrixCase matrixB isZero

//             logger.debug (
//                 eventX "Left matrix is \n{matrix}"
//                 >> setField "matrix" left
//             )

//             logger.debug (
//                 eventX "Right matrix is \n{matrix}"
//                 >> setField "matrix" right
//             )

//             graphblas {
//                 let! result = Matrix.mxm semiring left right
//                 let! tuples = Matrix.tuples result
//                 do! MatrixTuples.synchronize tuples
//                 return tuples
//             }
//             |> EvalGB.withClContext oclContext
//             |> EvalGB.runSync

//         finally
//             oclContext.Provider.CloseAllBuffers()

//     let expected = mxmNaive matrixA matrixB
//     let actual = mxmGB matrixA matrixB

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
        arbitrary = [typeof<PairOfMatricesOfCompatibleSize>]
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
            case.LeftMatrixCase = CSR &&
            case.RightMatrixCase = CSR &&
            case.MaskCase = NoMask
        )
    |> List.collect testFixtures
    |> testList "Mxm tests"
