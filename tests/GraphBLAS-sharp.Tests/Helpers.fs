namespace GraphBLAS.FSharp.Tests

open Brahma.FSharp.OpenCL.Translator
open FsCheck
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection
open Brahma.FSharp
open OpenCL.Net
open Expecto.Logging
open Expecto.Logging.Message
open System.Text.RegularExpressions
open FSharp.Quotations.Evaluator
open Expecto
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects

[<AutoOpen>]
module Extensions =
    type ClosedBinaryOp<'a> with
        member this.Invoke =
            let (ClosedBinaryOp f) = this
            QuotationEvaluator.Evaluate f

module CustomDatatypes =
    // мб заменить рекорд на структуру (не помогает)
    [<Struct>]
    type WrappedInt =
        { InnerValue: int }
        static member (+)(x: WrappedInt, y: WrappedInt) =
            { InnerValue = x.InnerValue + y.InnerValue }

        static member (*)(x: WrappedInt, y: WrappedInt) =
            { InnerValue = x.InnerValue * y.InnerValue }

    let addMultSemiringOnWrappedInt: Semiring<WrappedInt> =
        { PlusMonoid =
              { AssociativeOp = ClosedBinaryOp <@ (+) @>
                Identity = { InnerValue = 0 } }

          TimesSemigroup = { AssociativeOp = ClosedBinaryOp <@ (*) @> } }

module Generators =
    let logger = Log.create "Generators"

    // TODO уточнить, что пустые матрицы тоже генерятся
    let dimension2DGenerator =
        Gen.sized
        <| fun size -> Gen.choose (1, size) |> Gen.two

    let dimension3DGenerator =
        Gen.sized
        <| fun size -> Gen.choose (1, size) |> Gen.three

    let genericSparseGenerator zero valuesGen handler =
        let maxSparsity = 100
        let sparsityGen = Gen.choose (0, maxSparsity)

        let genWithSparsity sparseValuesGenProvider =
            gen {
                let! sparsity = sparsityGen

                logger.debug (
                    eventX "Sparcity is {sp} of {ms}"
                    >> setField "sp" sparsity
                    >> setField "ms" maxSparsity
                )

                return! sparseValuesGenProvider sparsity
            }

        genWithSparsity
        <| fun sparsity ->
            [ (maxSparsity - sparsity, valuesGen)
              (sparsity, Gen.constant zero) ]
            |> Gen.frequency
            |> handler

    type SingleMatrix() =
        static let matrixGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrows, ncols = dimension2DGenerator
                let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
                return matrix
            }

        static member IntType() =
            matrixGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            matrixGenerator
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
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

    type SingleSymmetricalMatrix() =
        static let matrixGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrows, _ = dimension2DGenerator
                let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, nrows)

                for row in 1 .. nrows - 1 do
                    for col in 0 .. row - 1 do
                        matrix.[row, col] <- matrix.[col, row]

                return matrix
            }

        static member IntType() =
            matrixGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            matrixGenerator
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
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
        static let pairOfMatricesOfEqualSizeGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrows, ncols = dimension2DGenerator
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
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
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
        static let pairOfMatrixAndVectorOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrows, ncols = dimension2DGenerator
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
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
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

        static member WrappedInt() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator
                CustomDatatypes.addMultSemiringOnWrappedInt.PlusMonoid.Identity
                Arb.generate<CustomDatatypes.WrappedInt>
            |> Arb.fromGen

    type PairOfSparseVectorAndMatrixOfCompatibleSize() =
        static let pairOfVectorAndMatrixOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrows, ncols = dimension2DGenerator
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
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
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
        static let pairOfMatricesOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrowsA, ncolsA, ncolsB = dimension3DGenerator

                let! matrixA =
                    valuesGenerator
                    |> Gen.array2DOfDim (nrowsA, ncolsA)

                let! matrixB =
                    valuesGenerator
                    |> Gen.array2DOfDim (ncolsA, ncolsB)

                return (matrixA, matrixB)
            }

        static member IntType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0 Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
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

    type PairOfMatricesOfCompatibleSizeWithMask() =
        static let pairOfMatricesOfCompatibleSizeWithMaskGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nrowsA, ncolsA, ncolsB = dimension3DGenerator

                let! matrixA =
                    valuesGenerator
                    |> Gen.array2DOfDim (nrowsA, ncolsA)

                let! matrixB =
                    valuesGenerator
                    |> Gen.array2DOfDim (ncolsA, ncolsB)

                let! mask =
                    (genericSparseGenerator false Arb.generate<bool>)
                    <| Gen.array2DOfDim (nrowsA, ncolsB)

                return (matrixA, matrixB, mask)
            }

        static member IntType() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> (genericSparseGenerator 0 Arb.generate<int>)
            |> Arb.fromGen

        static member FloatType() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator
                0.
                (Arb.Default.NormalFloat()
                 |> Arb.toGen
                 |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0y Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0uy Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0s Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0us Arb.generate<uint16>
            |> Arb.fromGen

        static member Int32Type() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0 Arb.generate<int32>
            |> Arb.fromGen

        static member UInt32Type() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0u Arb.generate<uint32>
            |> Arb.fromGen

        static member BoolType() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator false Arb.generate<bool>
            |> Arb.fromGen

    type ArrayOfDistinctKeys() =
        static let arrayOfDistinctKeysGenerator (keysGenerator: Gen<'n>) (valuesGenerator: Gen<'a>) =
            let tuplesGenerator =
                Gen.zip3
                <| keysGenerator
                <| keysGenerator
                <| valuesGenerator

            gen {
                let! length = Gen.sized <| fun size -> Gen.choose (1, size)

                let! array = Gen.arrayOfLength <| length <| tuplesGenerator

                return Array.distinctBy (fun (r, c, _) -> r, c) array
            }

        static member IntType() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| (Arb.Default.NormalFloat()
                |> Arb.toGen
                |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<uint16>
            |> Arb.fromGen

        static member Int32Type() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<int32>
            |> Arb.fromGen

        static member UInt32Type() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<uint32>
            |> Arb.fromGen

        static member BoolType() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| Arb.generate<bool>
            |> Arb.fromGen

    type ArrayOfAscendingKeys() =
        static let arrayOfAscendingKeysGenerator (valuesGenerator: Gen<'a>) =
            let tuplesGenerator =
                Gen.zip
                <| (Gen.sized <| fun size -> Gen.choose (0, size))
                <| valuesGenerator

            gen {
                let! length = Gen.sized <| fun size -> Gen.choose (1, size)

                let! array = Gen.arrayOfLength <| length <| tuplesGenerator

                return array |> Array.sortBy fst
            }

        static member IntType() =
            arrayOfAscendingKeysGenerator <| Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            arrayOfAscendingKeysGenerator
            <| (Arb.Default.NormalFloat()
                |> Arb.toGen
                |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<uint16>
            |> Arb.fromGen

        static member Int32Type() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<int32>
            |> Arb.fromGen

        static member UInt32Type() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<uint32>
            |> Arb.fromGen

        static member BoolType() =
            arrayOfAscendingKeysGenerator
            <| Arb.generate<bool>
            |> Arb.fromGen

    type BufferCompatibleVector() =
        static let pairOfVectorsOfEqualSize (valuesGenerator: Gen<'a>) =
            gen {
                let! length = Gen.sized <| fun size -> Gen.choose (1, size)

                let! array = Gen.arrayOfLength length valuesGenerator

                return array
            }

        static member IntType() =
            pairOfVectorsOfEqualSize <| Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfVectorsOfEqualSize
            <| (Arb.Default.NormalFloat()
                |> Arb.toGen
                |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfVectorsOfEqualSize <| Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfVectorsOfEqualSize <| Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<uint16>
            |> Arb.fromGen

        static member Int32Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<int32>
            |> Arb.fromGen

        static member UInt32Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<uint32>
            |> Arb.fromGen

        static member BoolType() =
            pairOfVectorsOfEqualSize <| Arb.generate<bool>
            |> Arb.fromGen

    type PairOfVectorsOfEqualSize() =
        static let pairOfVectorsOfEqualSize (valuesGenerator: Gen<'a>) =
            gen {
                let! length = Gen.sized <| fun size -> Gen.choose (1, size)

                let! leftArray = Gen.arrayOfLength length valuesGenerator

                let! rightArray = Gen.arrayOfLength length valuesGenerator

                return (leftArray, rightArray)
            }

        static member IntType() =
            pairOfVectorsOfEqualSize <| Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            pairOfVectorsOfEqualSize
            <| (Arb.Default.NormalFloat()
                |> Arb.toGen
                |> Gen.map float)
            |> Arb.fromGen

        static member SByteType() =
            pairOfVectorsOfEqualSize <| Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            pairOfVectorsOfEqualSize <| Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<uint16>
            |> Arb.fromGen

        static member Int32Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<int32>
            |> Arb.fromGen

        static member UInt32Type() =
            pairOfVectorsOfEqualSize <| Arb.generate<uint32>
            |> Arb.fromGen

        static member BoolType() =
            pairOfVectorsOfEqualSize <| Arb.generate<bool>
            |> Arb.fromGen

module Utils =

    let defaultConfig =
        { FsCheckConfig.defaultConfig with
              maxTest = 10
              startSize = 1
              endSize = 1000
              arbitrary =
                  [ typeof<Generators.SingleMatrix>
                    typeof<Generators.PairOfSparseMatricesOfEqualSize>
                    typeof<Generators.PairOfMatricesOfCompatibleSize>
                    typeof<Generators.PairOfSparseMatrixOAndVectorfCompatibleSize>
                    typeof<Generators.PairOfSparseVectorAndMatrixOfCompatibleSize>
                    typeof<Generators.ArrayOfDistinctKeys>
                    typeof<Generators.ArrayOfAscendingKeys>
                    typeof<Generators.BufferCompatibleVector>
                    typeof<Generators.PairOfVectorsOfEqualSize> ] }

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute
        || x.Equals y

    let vectorToDenseVector =
        function
        | Vector.Dense vector -> vector
        | _ -> failwith "Vector format must be Dense."

    let undirectedAlgoConfig =
        { FsCheckConfig.defaultConfig with
              maxTest = 10
              startSize = 1
              endSize = 1000
              arbitrary = [ typeof<Generators.SingleSymmetricalMatrix> ] }

    let createMatrixFromArray2D matrixCase array isZero =
        match matrixCase with
        | CSR ->
            Matrix.CSR
            <| Matrix.CSR.FromArray2D(array, isZero)
        | COO ->
            Matrix.COO
            <| Matrix.COO.FromArray2D(array, isZero)
        | CSC ->
            Matrix.CSC
            <| Matrix.CSC.FromArray2D(array, isZero)

    let createVectorFromArray vectorCase array isZero =
        match vectorCase with
        | VectorFormat.Sparse ->
            Vector.Sparse
            <| Vector.Sparse.FromArray(array, isZero)
        | VectorFormat.Dense ->
            Vector.Dense
            <| ArraysExtensions.DenseVectorFromArray(array, isZero)

    let createArrayFromDictionary size zero (dictionary: System.Collections.Generic.Dictionary<int, 'a>) =
        let array = Array.create size zero

        for key in dictionary.Keys do
            array.[key] <- dictionary.[key]

        array

    let unwrapOptionArray zero array =
        Array.map
            (fun x ->
                match x with
                | Some v -> v
                | None -> zero)
            array

    let compareArrays areEqual (actual: 'a []) (expected: 'a []) message =
        sprintf "%s. Lengths should be equal. Actual is %A, expected %A" message actual expected
        |> Expect.equal actual.Length expected.Length

        for i in 0 .. actual.Length - 1 do
            if not (areEqual actual.[i] expected.[i]) then
                sprintf "%s. Arrays differ at position %A of %A. Actual value is %A, expected %A"
                <| message
                <| i
                <| (actual.Length - 1)
                <| actual.[i]
                <| expected.[i]
                |> failtestf "%s"

    let listOfUnionCases<'a> =
        FSharpType.GetUnionCases typeof<'a>
        |> Array.map (fun caseInfo -> FSharpValue.MakeUnion(caseInfo, [||]) :?> 'a)
        |> List.ofArray

    let rec cartesian listOfLists =
        match listOfLists with
        | [ x ] -> List.fold (fun acc elem -> [ elem ] :: acc) [] x
        | h :: t ->
            List.fold
                (fun cacc celem ->
                    (List.fold (fun acc elem -> (elem :: celem) :: acc) [] h)
                    @ cacc)
                []
                (cartesian t)
        | _ -> []

module Context =
    type TestContext =
        { ClContext: ClContext
          Queue: MailboxProcessor<Msg> }

    let availableContexts (platformRegex: string) =
        let mutable e = ErrorCode.Unknown

        Cl.GetPlatformIDs &e
        |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, DeviceType.All, &e))
        |> Seq.ofArray
        |> Seq.distinctBy
            (fun device ->
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Name, &e)
                    .ToString())
        |> Seq.filter
            (fun device ->
                let isAvaliable =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Available, &e)
                        .CastTo<bool>()

                let platform =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                        .CastTo<Platform>()

                let platformName =
                    Cl
                        .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                        .ToString()

                (Regex platformRegex).IsMatch platformName
                && isAvaliable)
        |> Seq.map
            (fun device ->
                let platform =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                        .CastTo<Platform>()

                let clPlatform =
                    Cl
                        .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                        .ToString()
                    |> Platform.Custom

                let deviceType =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Type, &e)
                        .CastTo<DeviceType>()

                let _ =
                    match deviceType with
                    | DeviceType.Cpu -> ClDeviceType.Cpu
                    | DeviceType.Gpu -> ClDeviceType.Gpu
                    | DeviceType.Default -> ClDeviceType.Default
                    | _ -> failwith "Unsupported"

                let device =
                    ClDevice.GetFirstAppropriateDevice(clPlatform)

                let translator = FSQuotationToOpenCLTranslator device

                let context = ClContext(device, translator)
                let queue = context.QueueProvider.CreateQueue()

                { ClContext = context; Queue = queue })

    let defaultContext =
        let device = ClDevice.GetFirstAppropriateDevice()

        let context =
            ClContext(device, FSQuotationToOpenCLTranslator device)

        let queue = context.QueueProvider.CreateQueue()

        { ClContext = context; Queue = queue }

    let gpuOnlyContextFilter =
        Seq.filter
            (fun (context: TestContext) ->
                let mutable e = ErrorCode.Unknown
                let device = context.ClContext.ClDevice.Device

                let deviceType =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Type, &e)
                        .CastTo<DeviceType>()

                deviceType = DeviceType.Gpu)

module TestCases =

    type OperationCase<'a> =
        { TestContext: Context.TestContext
          Format: 'a }

    let defaultPlatformRegex = ""

    let testCases contextFilter =
        Context.availableContexts defaultPlatformRegex
        |> contextFilter
        |> List.ofSeq

    let getTestCases<'a> contextFilter =
        Context.availableContexts defaultPlatformRegex
        |> contextFilter
        |> List.ofSeq
        |> List.collect
            (fun x ->
                Utils.listOfUnionCases<'a>
                |> List.ofSeq
                |> List.map (fun y -> x, y))
        |> List.map
            (fun pair ->
                { TestContext = fst pair
                  Format = snd pair })

    let operationGPUTests name (testFixtures: OperationCase<'a> -> Test list) =
        getTestCases<'a> Context.gpuOnlyContextFilter
        |> List.distinctBy (fun case -> case.TestContext.ClContext, case.Format)
        |> List.collect testFixtures
        |> testList name

    let gpuTests name testFixtures =
        testCases Context.gpuOnlyContextFilter
        |> List.distinctBy (fun testContext -> testContext.ClContext)
        |> List.collect testFixtures
        |> testList name
