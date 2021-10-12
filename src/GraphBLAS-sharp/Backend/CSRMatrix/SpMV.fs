namespace rec GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open Brahma.OpenCL

// let pcsr (matrix: CSRMatrix<'a>) (vector: BitmapVector<'a>) mask (semiring: ISemiring<'a>) = opencl {
//     let (ClosedBinaryOp plus) = semiring.Plus
//     let (ClosedBinaryOp times) = semiring.Times

//     let matrixLength = matrix.Values.Length

//     let kernel1 =
//         <@
//             fun (ndRange: _1D)
//                 (matrixColumns: int[])
//                 (matrixValues: 'a[])
//                 (vectorBitmap: bool[])
//                 (vectorValues: 'a[])
//                 (intermediateArray: 'a[]) ->

//                 let i = ndRange.GlobalID0
//                 if i < matrixLength && vectorBitmap.[i] then
//                     let value = matrixValues.[i]
//                     let column = matrixColumns.[i]
//                     intermediateArray.[i] <- (%times) value vectorValues.[column]
//         @>

//     let kernel2 =
//         <@
//             fun (ndRange: _1D)
//                 (intermediateArray: 'a[])
//                 (matrixPtr: int[])
//                 (outputVector: 'a[]) ->

//                 let gid = ndRange.GlobalID0
//                 let lid = ndRange.LocalID0

//                 let localPtr = localArray<int> (Utils.defaultWorkGroupSize + 1)
//                 localPtr.[lid] <- matrixPtr.[gid]
//                 if lid = 0 then
//                     localPtr.[Utils.defaultWorkGroupSize] <- matrixPtr.[gid + Utils.defaultWorkGroupSize]
//                 barrier ()
//         @>

//     let intermediateArray = Array.zeroCreate<'a> matrixLength
//     do! RunCommand kernel1 <| fun kernelPrepare ->
//         let range = _1D(Utils.getDefaultGlobalSize matrixLength, Utils.defaultWorkGroupSize)
//         kernelPrepare
//             range
//             matrix.ColumnIndices
//             matrix.Values
//             vector.Bitmap
//             vector.Values
//             intermediateArray

//     let outputVector = Array.zeroCreate<'a> matrix.RowCount
//     do! RunCommand kernel2 <| fun kernelPrepare ->
//         let range = _1D(Utils.getDefaultGlobalSize matrixLength, Utils.defaultWorkGroupSize)
//         kernelPrepare
//             range
//             intermediateArray
//             matrix.RowPointers
//             outputVector

//     return {
//         Bitmap = vector.Bitmap
//         Values = outputVector
//     }
// }
