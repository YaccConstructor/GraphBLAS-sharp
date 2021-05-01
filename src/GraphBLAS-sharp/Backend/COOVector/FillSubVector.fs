namespace GraphBLAS.FSharp.Backend.COOVector

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.COOVector.Utilities

module internal FillSubVector =
    let private runNotEmpty (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (scalar: 'a[]) : OpenCLEvaluation<int[] * 'a[]> = opencl {
        let! allIndices, allValues = mergeWithScalar leftIndices leftValues rightIndices scalar

        let! rawPositions = preparePositionsWithoutPlus allIndices

        return! setPositions allIndices allValues rawPositions
    }

    let run (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (scalar: 'a[]) : OpenCLEvaluation<int[] * 'a[]> =
        if leftValues.Length = 0 then
            opencl {
                let! resultIndices = Copy.run rightIndices
                let! resultValues = Replicate.run rightIndices.Length scalar

                return resultIndices, resultValues
            }

        elif rightIndices.Length = 0 then
            opencl {
                return leftIndices, leftValues
            }

        else
            runNotEmpty leftIndices leftValues rightIndices scalar
