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
open FSharp.Quotations.Evaluator

[<AutoOpen>]
module Extensions =
    type ClosedBinaryOp<'a> with
        member this.Eval =
            let (ClosedBinaryOp f) = this
            QuotationEvaluator.Evaluate f

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

    let pairOfMatricesOfEqualSizeGenerator (valuesGenerator: Gen<'a>) = gen {
        let! (nrows, ncols) = dimension2DGenerator
        let! matrixA = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
        let! matrixB = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
        return (matrixA, matrixB)
    }

    let pairOfVectorAndMatrixOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) = gen {
        let! (nrows, ncols) = dimension2DGenerator
        let! vector = valuesGenerator |> Gen.arrayOfLength nrows
        let! matrix = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
        let! mask = Arb.generate<bool> |> Gen.arrayOfLength ncols
        return (vector, matrix, mask)
    }

    let pairOfMatricesOfCompatibleSizeGenerator (valuesGenerator: Gen<'a>) = gen {
        let! (nrowsA, ncolsA, ncolsB) = dimension3DGenerator
        let! matrixA = valuesGenerator |> Gen.array2DOfDim (nrowsA, ncolsA)
        let! matrixB = valuesGenerator |> Gen.array2DOfDim (ncolsA, ncolsB)
        return (matrixA, matrixB)
    }

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
