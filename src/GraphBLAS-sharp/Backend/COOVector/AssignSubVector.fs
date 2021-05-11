namespace GraphBLAS.FSharp.Backend.COOVector

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.COOVector.Utilities
open GraphBLAS.FSharp.Backend.COOVector.Utilities.AssignSubVector

module internal AssignSubVector =
    let private runNotEmpty (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (rightValues: 'a[]) (maskIndices: int[]) : OpenCLEvaluation<int[] * 'a[]> = opencl {
        let! bitmap, maskValues = intersect rightIndices rightValues maskIndices

        let! resultIndices, resultValues, rawPositions = filter leftIndices leftValues maskIndices maskValues bitmap

        let! rawPositions = preparePositions resultIndices rawPositions

        return! setPositions resultIndices resultValues rawPositions
    }

    let run (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (rightValues: 'a[]) (maskIndices: int[]) : OpenCLEvaluation<int[] * 'a[]> =
        if leftValues.Length = 0 then
            opencl {
                let! resultIndices = Copy.run rightIndices
                let! resultValues = Copy.run rightValues

                return resultIndices, resultValues
            }

        elif rightIndices.Length = 0 then
            opencl {
                return leftIndices, leftValues
            }

        else
            runNotEmpty leftIndices leftValues rightIndices rightValues maskIndices
