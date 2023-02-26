namespace GraphBLAS.FSharp.Test

open FsCheck
open GraphBLAS.FSharp
open Expecto.Logging
open Expecto.Logging.Message
open FSharp.Quotations.Evaluator

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

    /// <remarks>
    /// Generates empty matrices as well.
    /// </remarks>
    let dimension2DGenerator =
        Gen.sized
        <| fun size -> Gen.choose (1, size) |> Gen.two

    let dimension3DGenerator =
        Gen.sized
        <| fun size -> Gen.choose (1, size) |> Gen.three

    let rec normalFloat32Generator (random: System.Random) =
        gen {
            let result = random.NextSingle()

            if System.Single.IsNormal result then
                return result
            else
                return! normalFloat32Generator random
        }

    let genericSparseGenerator zero valuesGen handler =
        let maxSparsity = 100
        let sparsityGen = Gen.choose (0, maxSparsity)

        let genWithSparsity sparseValuesGenProvider =
            gen {
                let! sparsity = sparsityGen

                logger.debug (
                    eventX "Sparsity is {sp} of {ms}"
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

        static member Float32Type() =
            matrixGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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
                let! nRows, _ = dimension2DGenerator
                let! matrix = valuesGenerator |> Gen.array2DOfDim (nRows, nRows)

                for row in 1 .. nRows - 1 do
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

        static member Float32Type() =
            matrixGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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
                let! nRows, nColumns = dimension2DGenerator

                let! matrixA =
                    valuesGenerator
                    |> Gen.array2DOfDim (nRows, nColumns)

                let! matrixB =
                    valuesGenerator
                    |> Gen.array2DOfDim (nRows, nColumns)

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

        static member Float32Type() =
            pairOfMatricesOfEqualSizeGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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

    type PairOfSparseMatrixAndVectorsCompatibleSize() =
        static let pairOfMatrixAndVectorOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) =
            gen {
                let! nRows, nColumns = dimension2DGenerator

                let! matrix =
                    valuesGenerator
                    |> Gen.array2DOfDim (nRows, nColumns)

                let! vector = valuesGenerator |> Gen.arrayOfLength nColumns
                let! mask = Arb.generate<bool> |> Gen.arrayOfLength nRows
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

        static member Float32Type() =
            pairOfMatrixAndVectorOfCompatibleSizeGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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
                let! nRows, nColumns = dimension2DGenerator
                let! vector = valuesGenerator |> Gen.arrayOfLength nRows

                let! matrix =
                    valuesGenerator
                    |> Gen.array2DOfDim (nRows, nColumns)

                let! mask = Arb.generate<bool> |> Gen.arrayOfLength nColumns
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

        static member Float32Type() =
            pairOfVectorAndMatrixOfCompatibleSizeGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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
                let! nRowsA, nColsA, nColsB = dimension3DGenerator

                let! matrixA =
                    valuesGenerator
                    |> Gen.array2DOfDim (nRowsA, nColsA)

                let! matrixB =
                    valuesGenerator
                    |> Gen.array2DOfDim (nColsA, nColsB)

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

        static member Float32Type() =
            pairOfMatricesOfCompatibleSizeGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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
                let! nRowsA, nColumnsA, nColumnsB = dimension3DGenerator

                let! matrixA =
                    valuesGenerator
                    |> Gen.array2DOfDim (nRowsA, nColumnsA)

                let! matrixB =
                    valuesGenerator
                    |> Gen.array2DOfDim (nColumnsA, nColumnsB)

                let! mask =
                    (genericSparseGenerator false Arb.generate<bool>)
                    <| Gen.array2DOfDim (nRowsA, nColumnsB)

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

        static member Float32Type() =
            pairOfMatricesOfCompatibleSizeWithMaskGenerator
            |> genericSparseGenerator 0.0f (normalFloat32Generator <| System.Random())
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

        static member Float32Type() =
            arrayOfDistinctKeysGenerator
            <| Arb.generate<int>
            <| (normalFloat32Generator <| System.Random())
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

        static member Float32Type() =
            arrayOfAscendingKeysGenerator
            <| (normalFloat32Generator <| System.Random())
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

    type BufferCompatibleArray() =
        static let compatibleVector (valuesGenerator: Gen<'a>) =
            gen {
                let! length = Gen.sized <| fun size -> Gen.choose (1, size)

                let! array = Gen.arrayOfLength length valuesGenerator

                return array
            }

        static member IntType() =
            compatibleVector <| Arb.generate<int>
            |> Arb.fromGen

        static member FloatType() =
            compatibleVector
            <| (Arb.Default.NormalFloat()
                |> Arb.toGen
                |> Gen.map float)
            |> Arb.fromGen

        static member Float32Type() =
            compatibleVector
            <| (normalFloat32Generator <| System.Random())
            |> Arb.fromGen

        static member SByteType() =
            compatibleVector <| Arb.generate<sbyte>
            |> Arb.fromGen

        static member ByteType() =
            compatibleVector <| Arb.generate<byte>
            |> Arb.fromGen

        static member Int16Type() =
            compatibleVector <| Arb.generate<int16>
            |> Arb.fromGen

        static member UInt16Type() =
            compatibleVector <| Arb.generate<uint16>
            |> Arb.fromGen

        static member Int32Type() =
            compatibleVector <| Arb.generate<int32>
            |> Arb.fromGen

        static member UInt32Type() =
            compatibleVector <| Arb.generate<uint32>
            |> Arb.fromGen

        static member BoolType() =
            compatibleVector <| Arb.generate<bool>
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

        static member Float32Type() =
            pairOfVectorsOfEqualSize
            <| (normalFloat32Generator <| System.Random())
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

    type PairOfArraysAndValue() =
        static let pairOfVectorsOfEqualSize (valuesGenerator: Gen<'a>) =
            gen {
                let! length = Gen.sized <| fun size -> Gen.choose (1, size)

                let! leftArray = Gen.arrayOfLength length valuesGenerator

                let! rightArray = Gen.arrayOfLength length valuesGenerator

                let value =
                    List.last <| Gen.sample 100 1 valuesGenerator

                return (leftArray, rightArray, value)
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

        static member Float32Type() =
            pairOfVectorsOfEqualSize
            <| (normalFloat32Generator <| System.Random())
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
