namespace GraphBLAS.FSharp.Tests

open FsCheck
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net
open Expecto.Logging
open Expecto.Logging.Message
open System.Text.RegularExpressions
open FSharp.Quotations.Evaluator
open Expecto
open GraphBLAS.FSharp.Predefined

[<AutoOpen>]
module Extensions =
    type ClosedBinaryOp<'a> with
        member this.Invoke =
            let (ClosedBinaryOp f) = this
            QuotationEvaluator.Evaluate f

module Generators =
    let logger = Log.create "Generators"

    // TODO уточнить, что пустые матрицы тоже генерятся
    let dimension2DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (1, size)
            |> Gen.two

    let dimension3DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (1, size)
            |> Gen.three

    let genericSparseGenerator zero valuesGen handler =
        let maxSparsity = 100
        let sparsityGen = Gen.choose (0, maxSparsity)
        let genWithSparsity sparseValuesGenProvider = gen {
            let! sparsity = sparsityGen

            logger.debug (
                eventX "Sparcity is {sp} of {ms}"
                >> setField "sp" sparsity
                >> setField "ms" maxSparsity
            )

            return! sparseValuesGenProvider sparsity
        }

        genWithSparsity <| fun sparsity ->
            [
                (maxSparsity - sparsity, valuesGen)
                (sparsity, Gen.constant zero)
            ]
            |> Gen.frequency
            |> handler

    type SingleMatrix() =
        static let matrixGenerator (valuesGenerator: Gen<'a>) = gen {
            let! (nrows, ncols) = dimension2DGenerator
            let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            return matrix
        }

        static member IntType() =
            matrixGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            matrixGenerator
            |> genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            matrixGenerator
            |> genericSparseGenerator 0y Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            matrixGenerator
            |> genericSparseGenerator 0uy Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            matrixGenerator
            |> genericSparseGenerator 0s Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            matrixGenerator
            |> genericSparseGenerator 0us Arb.generate<uint16>
            |> Arb.fromGen

        static member BoolType() =
            matrixGenerator
            |> genericSparseGenerator false Arb.generate<bool>
            |> Arb.fromGen

    type PairOfSparseMatricesOfEqualSize() =
        static let pairOfMatricesOfEqualSizeGenerator (valuesGenerator: Gen<'a>) = gen {
            let! (nrows, ncols) = dimension2DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            let! matrixB = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            return (matrixA, matrixB)
        }

        static member IntType() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0y Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0uy Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0s Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0us Arb.generate<uint16>
            |> Arb.fromGen

        static member BoolType() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator false Arb.generate<bool>
            |> Arb.fromGen

    type PairOfSparseMatrixOAndVectorfCompatibleSize() =
        static let pairOfMatrixAndVectorOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) = gen {
            let! (nrows, ncols) = dimension2DGenerator
            let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            let! vector = valuesGenerator |> Gen.arrayOfLength ncols
            let! mask = Arb.generate<bool> |> Gen.arrayOfLength nrows
            return (matrix, vector, mask)
        }

        static member IntType() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0y Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0uy Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0s Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0us Arb.generate<uint16>
            |> Arb.fromGen

        static member BoolType() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator false Arb.generate<bool>
            |> Arb.fromGen

    type PairOfSparseVectorAndMatrixOfCompatibleSize() =
        static let pairOfVectorAndMatrixOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) = gen {
            let! (nrows, ncols) = dimension2DGenerator
            let! vector = valuesGenerator |> Gen.arrayOfLength nrows
            let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            let! mask = Arb.generate<bool> |> Gen.arrayOfLength ncols
            return (vector, matrix, mask)
        }

        static member IntType() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0y Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0uy Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0s Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0us Arb.generate<uint16>
            |> Arb.fromGen

        static member BoolType() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator false Arb.generate<bool>
            |> Arb.fromGen

    type PairOfMatricesOfCompatibleSize() =
        static let pairOfMatricesOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) = gen {
            let! (nrowsA, ncolsA, ncolsB) = dimension3DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (nrowsA, ncolsA)
            let! matrixB = valuesGenerator |> Gen.array2DOfDim (ncolsA, ncolsB)
            return (matrixA, matrixB)
        }

        static member IntType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0. (Arb.Default.NormalFloat() |> Arb.toGen |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0y Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0uy Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0s Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0us Arb.generate<uint16>
            |> Arb.fromGen

        static member BoolType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator false Arb.generate<bool>
            |> Arb.fromGen

    // type ArrayOfDistinctKeys() =
    //     // Stack overflow.
    //     static member ArrayOfDistinctKeysArb() =
    //         gen {
    //             let! array = Arb.generate<(uint64 * int)[]>
    //             return Array.distinctBy (fun (key, _) -> key) array
    //         }
    //         |> Arb.fromGen

module Utils =
    let defaultConfig =
        { FsCheckConfig.defaultConfig with
            maxTest = 10
            startSize = 1
            endSize = 1000
            arbitrary = [
                typeof<Generators.SingleMatrix>
                typeof<Generators.PairOfSparseMatricesOfEqualSize>
                typeof<Generators.PairOfMatricesOfCompatibleSize>
                typeof<Generators.PairOfSparseMatrixOAndVectorfCompatibleSize>
                typeof<Generators.PairOfSparseVectorAndMatrixOfCompatibleSize>
                // typeof<Generators.ArrayOfDistinctKeys>
            ]
        }

    let rec cartesian listOfLists =
        match listOfLists with
        | [x] -> List.fold (fun acc elem -> [elem] :: acc) [] x
        | h :: t ->
            List.fold (fun cacc celem ->
                (List.fold (fun acc elem -> (elem :: celem) :: acc) [] h) @ cacc
            ) [] (cartesian t)
        | _ -> []

    let listOfUnionCases<'a> =
        FSharpType.GetUnionCases typeof<'a>
        |> Array.map (fun caseInfo -> FSharpValue.MakeUnion(caseInfo, [||]) :?> 'a)
        |> List.ofArray

    let avaliableContexts (platformRegex: string) =
        let mutable e = ErrorCode.Unknown
        Cl.GetPlatformIDs &e
        |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, DeviceType.All, &e))
        |> Seq.ofArray
        |> Seq.distinctBy (fun device -> Cl.GetDeviceInfo(device, DeviceInfo.Name, &e).ToString())
        |> Seq.filter
            (fun device ->
                let isAvaliable = Cl.GetDeviceInfo(device, DeviceInfo.Available, &e).CastTo<bool>()
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                (Regex platformRegex).IsMatch platformName &&
                isAvaliable
            )
        |> Seq.map
            (fun device ->
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()
                OpenCLEvaluationContext(platformName, deviceType)
            )

    let createMatrixFromArray2D matrixCase array isZero =
        match matrixCase with
        | CSR -> MatrixCSR <| CSRMatrix.FromArray2D(array, isZero)
        | COO -> MatrixCOO <| COOMatrix.FromArray2D(array, isZero)

    let createVectorFromArray vectorCase array isZero =
        match vectorCase with
        | VectorFormat.COO -> VectorCOO <| COOVector.FromArray(array, isZero)
