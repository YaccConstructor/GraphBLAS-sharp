namespace CSRMultiplication.Tests

open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open CSRMultiplication
open OpenCL.Net
open Brahma.OpenCL
open GraphBLAS.FSharp

module SparseMatrixMultiplicationTests =
    ()

//     type MatrixMultiplicationPair =
//         static member FloatSparseMatricesPair () =
//             fun size ->
//                 let floatSparseGenerator =
//                     Gen.oneof [
//                         Arb.Default.NormalFloat () |> Arb.toGen |> Gen.map float
//                         Gen.constant 0.
//                     ]

//                 let dimGenerator =
//                     Gen.choose (0, size |> float |> sqrt |> int)
//                     |> Gen.three
//                     |> Gen.map (fun (m, n, k) -> ((m, n), (n, k)))

//                 gen {
//                     let! (leftDim, rightDim) = dimGenerator
//                     let! leftMatrix = floatSparseGenerator |> Gen.array2DOfDim leftDim
//                     let! rightMatrix = floatSparseGenerator |> Gen.array2DOfDim rightDim
//                     return (leftMatrix, rightMatrix)
//                 }
//                 |> Gen.filter (fun (left, right) -> left.Length <> 0 && right.Length <> 0)
//                 |> Gen.filter (fun (left, right) ->
//                     left |> Seq.cast<float> |> Seq.exists (fun elem -> abs elem > System.Double.Epsilon) &&
//                     right |> Seq.cast<float> |> Seq.exists (fun elem -> abs elem > System.Double.Epsilon))
//             |> Gen.sized
//             |> Arb.fromGen

//         static member FloatSparseMatrixVectorPair () =
//             fun size ->
//                 let floatSparseGenerator =
//                     Gen.oneof [
//                         Arb.Default.NormalFloat () |> Arb.toGen |> Gen.map float
//                         Gen.constant 0.
//                     ]

//                 let dimGenerator =
//                     Gen.choose (0, size |> float |> sqrt |> int)
//                     |> Gen.two

//                 gen {
//                     let! (rows, cols) = dimGenerator
//                     let! matrix = floatSparseGenerator |> Gen.array2DOfDim (rows, cols)
//                     let! vector = floatSparseGenerator |> Gen.arrayOfLength cols
//                     return (matrix, vector)
//                 }
//                 |> Gen.filter (fun (matrix, vector) -> matrix.Length <> 0 && vector.Length <> 0)
//                 |> Gen.filter (fun (matrix, vector) ->
//                     matrix |> Seq.cast<float> |> Seq.exists (fun elem -> abs elem > System.Double.Epsilon) &&
//                     vector |> Seq.cast<float> |> Seq.exists (fun elem -> abs elem > System.Double.Epsilon))
//             |> Gen.sized
//             |> Arb.fromGen

//     let getFlattenedDiff<'a when 'a :> System.Collections.IEnumerable> (result: 'a) (expected: 'a) =
//             let pairMap f (x, y) = f x, f y
//             (result, expected)
//             |> pairMap Seq.cast<float>
//             ||> Seq.zip
//             |> Seq.map (fun (x, y) -> x - y)

//     let checkEquality<'a when 'a :> System.Collections.IEnumerable> (result: 'a) (expected: 'a) =
//         getFlattenedDiff result expected
//         |> Seq.forall (fun diff -> diff < 1e-8)

//     let getLabel<'a when 'a :> System.Collections.IEnumerable> (result: 'a) (expected: 'a) =
//         sprintf "\n Total diff:\n %A\n Result:\n %A\n Expected:\n %A\n"
//             (getFlattenedDiff result expected |> Seq.sum)
//             result
//             expected

//     [<Properties(Verbose=false, MaxTest=100, EndSize=1600)>]
//     type Tests() =
//         let matrixVectorMultiply (vector: float[]) (matrix: float[,]) =
//             let rows = matrix |> Array2D.length1
//             let cols = matrix |> Array2D.length2
//             let resultVector = Array.zeroCreate<float> rows
//             for i in 0 .. rows - 1 do
//                 let mutable buffer = 0.
//                 for j in 0 .. cols - 1 do
//                     buffer <- buffer + matrix.[i, j] * vector.[j]
//                 resultVector.[i] <- buffer
//             resultVector

//         let matrixMatrixMultiply (right: float[,]) (left: float[,]) =
//             let rows = left |> Array2D.length1
//             let cols = right |> Array2D.length2
//             let resultMatrix = Array2D.zeroCreate<float> rows cols
//             for i in 0 .. rows - 1 do
//                 for j in 0 .. cols - 1 do
//                     let mutable buffer = 0.
//                     for k in 0 .. (left |> Array2D.length2) - 1 do
//                         buffer <- buffer + left.[i, k] * right.[k, j]
//                     resultMatrix.[i, j] <- resultMatrix.[i, j] + buffer
//             resultMatrix

//         let provider =
//             try ComputeProvider.Create("INTEL*", DeviceType.Cpu)
//             with
//             | ex -> failwith ex.Message
//         let commandQueue = new CommandQueue(provider, provider.Devices |> Seq.head)
//         let oclContext = {Provider = provider; CommandQueue = commandQueue}

//         [<Trait("Category", "vec")>]
//         [<Property(Arbitrary=[| typeof<MatrixMultiplicationPair> |])>]
//         member this.``CSR x Vector multiplication should work correctly on nonempty and nonzero objects`` (matrix: float[,], vector: float[]) =
//             let semiring =
//                 Semiring.create<float>
//                     LanguagePrimitives.GenericZero
//                     (BinaryOp <@ ( + ) @>)
//                     (BinaryOp <@ ( * ) @>)
//             let result = semiring |> CSRMatrix(matrix) +.* DenseVector(vector)
//             let expected = matrix |> matrixVectorMultiply vector
//             (checkEquality result.AsArray expected) |@ (getLabel result.AsArray expected)

        // [<Trait("Category", "dense")>]
        // [<Property(Arbitrary=[| typeof<MatrixMultiplicationPair> |])>]
        // member this.``CSR x Dense multiplication should work correctly on nonempty and nonzero matrices`` (left: float[,], right: float[,]) =
        //     let result = CSRMatrix.makeFromDenseMatrix left |> csrDenseMultiply right
        //     let expected = left |> matrixMatrixMultiply right
        //     (checkEquality result expected) |@ (getLabel result expected)

        // [<Trait("Category", "csc")>]
        // [<Property(Arbitrary=[| typeof<MatrixMultiplicationPair> |])>]
        // member this.``CSR x CSC multiplication should work correctly on nonempty and nonzero matrices`` (left: float[,], right: float[,]) =
        //     let result = CSRMatrix.makeFromDenseMatrix left |> csrCscMultiply (CSCMatrix.makeFromDenseMatrix right)
        //     let expected = left |> matrixMatrixMultiply right
        //     (checkEquality result expected) |@ (getLabel result expected)

        // [<Trait("Category", "csr")>]
        // [<Property(Arbitrary=[| typeof<MatrixMultiplicationPair> |])>]
        // member this.``CSR x CSR multiplication should work correctly on nonempty and nonzero matrices`` (left: float[,], right: float[,]) =
        //     let result = CSRMatrix.makeFromDenseMatrix left |> csrCsrMultiply (CSRMatrix.makeFromDenseMatrix right)
        //     let expected = left |> matrixMatrixMultiply right
        //     (checkEquality result expected) |@ (getLabel result expected)

        // [<Trait("Category", "csr-cpu")>]
        // [<Property(Arbitrary=[| typeof<MatrixMultiplicationPair> |])>]
        // member this.``CSR x CSR multiplication algo shoud work correctly on cpu`` (left: float[,], right: float[,]) =
        //     let csrMultAlgo (csrMatrixRight: CSRMatrix.CSRMatrix) (csrMatrixLeft: CSRMatrix.CSRMatrix) =
        //         let leftMatrixRowCount = csrMatrixLeft |> CSRMatrix.rowCount
        //         let leftMatrixColumnCount = csrMatrixLeft |> CSRMatrix.columnCount
        //         let rightMatrixRowCount = csrMatrixRight |> CSRMatrix.rowCount
        //         let rightMatrixColumnCount = csrMatrixRight |> CSRMatrix.columnCount
        //         if leftMatrixColumnCount <> rightMatrixRowCount then  failwith "fail"

        //         let leftCsrValuesBuffer = csrMatrixLeft.GetValues
        //         let leftCsrColumnsBuffer = csrMatrixLeft.GetColumns
        //         let leftCsrRowPointersBuffer = csrMatrixLeft.GetRowPointers
        //         let rightCsrValuesBuffer = csrMatrixRight.GetValues
        //         let rightCsrColumnsBuffer = csrMatrixRight.GetColumns
        //         let rightCsrRowPointersBuffer = csrMatrixRight.GetRowPointers

        //         let resultMatrix = Array2D.zeroCreate<float> leftMatrixRowCount rightMatrixColumnCount
        //         for i in 0 .. rightMatrixRowCount - 1 do
        //             for j in 0 .. leftMatrixRowCount - 1 do
        //                 for k in rightCsrRowPointersBuffer.[i] .. rightCsrRowPointersBuffer.[i + 1] - 1 do
        //                     let mutable localResultBuffer = resultMatrix.[j, rightCsrColumnsBuffer.[k]]
        //                     let mutable pointer = leftCsrRowPointersBuffer.[j]
        //                     while (pointer < leftCsrRowPointersBuffer.[j + 1] && leftCsrColumnsBuffer.[pointer] <= i) do
        //                         if leftCsrColumnsBuffer.[pointer] = i then
        //                             localResultBuffer <- localResultBuffer +
        //                                 rightCsrValuesBuffer.[k] * leftCsrValuesBuffer.[pointer]
        //                         pointer <- pointer + 1
        //                     resultMatrix.[j, rightCsrColumnsBuffer.[k]] <- localResultBuffer

        //         resultMatrix

        //     let result = CSRMatrix.makeFromDenseMatrix left |> csrMultAlgo (CSRMatrix.makeFromDenseMatrix right)
        //     let expected = left |> matrixMatrixMultiply right
        //     (checkEquality result expected) |@ (getLabel result expected)

        // interface System.IDisposable with
        //     member this.Dispose () =
        //         commandQueue.Dispose ()
        //         provider.CloseAllBuffers ()
        //         provider.Dispose ()
