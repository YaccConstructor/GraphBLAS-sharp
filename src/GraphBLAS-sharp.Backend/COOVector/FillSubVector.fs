namespace GraphBLAS.FSharp.Backend.COOVector

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.COOVector.Utilities
open GraphBLAS.FSharp.Backend.COOVector.Utilities.FillSubVector

module internal FillSubVector =
    let private runNotEmpty
        (leftIndices: int [])
        (leftValues: 'a [])
        (rightIndices: int [])
        (scalar: 'a [])
        : ClTask<int [] * 'a []> =
        opencl {
            let! allIndices, allValues = merge leftIndices leftValues rightIndices scalar

            let! rawPositions = preparePositions allIndices

            return! setPositions allIndices allValues rawPositions
        }

    let run (leftIndices: int []) (leftValues: 'a []) (rightIndices: int []) (scalar: 'a []) : ClTask<int [] * 'a []> =
        if leftValues.Length = 0 then
            opencl {
                let! resultIndices = Copy.copyArray rightIndices
                let! resultValues = Replicate.run rightIndices.Length scalar

                return resultIndices, resultValues
            }

        elif rightIndices.Length = 0 then
            opencl { return leftIndices, leftValues }

        else
            runNotEmpty leftIndices leftValues rightIndices scalar
