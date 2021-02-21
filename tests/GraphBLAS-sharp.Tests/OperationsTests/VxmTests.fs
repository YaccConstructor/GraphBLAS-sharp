module VxmTests

open Expecto
open FsCheck
open GraphBLAS.FSharp
open MathNet.Numerics
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GlobalContext

// let config = {
//     FsCheckConfig.defaultConfig with
//         arbitrary = [ typeof<MatrixMultiplicationPair> ]
// }

// let testCases =
//     [
//         typeof<VectorType>
//         typeof<MatrixType>
//         typeof<MaskType>
//     ]
//     |> List.map Utils.enumValues
//     |> Utils.cartesian
//     |> List.map (fun list ->
//         {
//             VectorCase = enum<VectorType> list.[0]
//             MatrixCase = enum<MatrixType> list.[1]
//             MaskCase = enum<MaskType> list.[2]
//         })

// let testsInStandardSemiring =
//     let stdSemiring = Predefined.FloatSemiring.addMult
//     ptestList "Float vector-matrix multiplication tests" (
//         List.collect (fun case ->
//             // добавить возможность пропускать некоторые случаи
//             let matrixBackend =
//                 match case.MatrixCase with
//                 | MatrixType.CSR -> CSR
//                 | _ -> failwith "Not Implemented"

//             let vectorConstructor =
//                 match case.VectorCase with
//                 | VectorType.Sparse -> (fun array -> Vector.Sparse(array, 0.))
//                 | _ -> failwith "Not Implemented"

//             let zeroVectorConstructor =
//                 match case.VectorCase with
//                 | VectorType.Sparse -> (fun length -> Vector.ZeroSparse(length))
//                 | _ -> failwith "Not Implemented"

//             let mask : Mask1D option =
//                 match case.MaskCase with
//                 | MaskType.None -> failwith "Not Implemented"
//                 | _ -> failwith "Not Implemented"

//             [
//                 testPropertyWithConfig config "Dimensional mismatch should raise an exception" <|
//                     fun matrixRowCount matrixColumnCount vectorSize ->
//                         let emptyMatrix = Matrix.zeroCreate matrixRowCount matrixColumnCount matrixBackend
//                         let emptyVector = zeroVectorConstructor vectorSize

//                         Expect.throwsT<System.ArgumentException>
//                             (fun () ->
//                                 opencl {
//                                     return! (emptyVector @. emptyMatrix) mask stdSemiring
//                                 }
//                                 |> oclContext.RunSync
//                                 |> ignore
//                             )
//                             (sprintf "1x%i @ %ix%i\n case:\n %A" vectorSize matrixRowCount matrixColumnCount case)

//                 testPropertyWithConfig config "Operation should have correct semantic" <|
//                     fun (denseMatrix: float[,]) (denseVector: float[]) ->
//                         let matrix = Matrix.ofArray2D denseMatrix ((=) 0.) matrixBackend
//                         let vector = vectorConstructor denseVector
//                         let result =
//                             opencl {
//                                 return! (vector @. matrix) mask stdSemiring
//                             }
//                             |> oclContext.RunSync
//                         let a = LinearAlgebra.DenseMatrix.ofArray2 denseMatrix
//                         let b = LinearAlgebra.DenseVector.ofArray denseVector
//                         let c = b * a
//                         let elementWiseDifference =
//                             (result |> Vector.toSeq, c.AsArray() |> Seq.ofArray)
//                             ||>  Seq.zip
//                             |> Seq.map (fun (a, b) -> a - b)

//                         Expect.all
//                             elementWiseDifference
//                             (fun diff -> abs diff < Accuracy.medium.absolute)
//                             (sprintf "%A @ %A = %A\n case:\n %A" vector matrix result case)

//                 ptestPropertyWithConfig config "Explicit zeroes after operation should be dropped" <|
//                     fun a b -> a + b = b + a
//             ]
//         ) testCases
//     )
