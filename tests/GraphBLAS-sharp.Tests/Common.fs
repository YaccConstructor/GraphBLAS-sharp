namespace GraphBLAS.FSharp.Tests

open FsCheck
open System
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net
open Expecto.Logging
open Expecto.Logging.Message
open System.Text.RegularExpressions

module Generators =
    let logger = Log.create "Generators"

    let dimension2DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (1, size |> float |> sqrt |> int)
            |> Gen.two

    let dimension3DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (1, size |> float |> sqrt |> int)
            |> Gen.three

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

        genWithSparsity <| fun sparsity ->
            [
                (maxSparsity - sparsity, valuesGen)
                (sparsity, Gen.constant zero)
            ]
            |> Gen.frequency
            |> handler

    // generate non-empty matrices
    let pairOfMatricesOfEqualSizeGenerator (valuesGenerator: Gen<'a>) =
        gen {
            let! (nrows, ncols) = dimension2DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            let! matrixB = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            return (matrixA, matrixB)
        }
        |> Gen.filter (fun (matrixA, matrixB) -> matrixA.Length <> 0 && matrixB.Length <> 0)

    let pairOfMatrixAndVectorOfEqualSizeGenerator (valuesGenerator: Gen<'a>) =
        gen {
            let! (nrows, ncols) = dimension2DGenerator
            let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            let! vector = valuesGenerator |> Gen.arrayOfLength ncols
            return (matrix, vector)
        }
        |> Gen.filter (fun (matrix, vector) -> matrix.Length <> 0 && vector.Length <> 0)

module Utils =
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
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                (Regex platformRegex).IsMatch platformName
            )
        |> Seq.map
            (fun device ->
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()
                OpenCLEvaluationContext(platformName, deviceType)
            )
