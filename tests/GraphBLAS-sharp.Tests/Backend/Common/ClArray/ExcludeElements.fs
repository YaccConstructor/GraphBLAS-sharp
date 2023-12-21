module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.ExcludeElements

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.ClArray.ExcludeElements> ] }

let makeTest<'a> isEqual (zero: 'a) testFun ((array, bitmap): 'a array * int array) =
    if array.Length > 0 && (Array.exists ((=) 1) bitmap) then

        let arrayCl = context.CreateClArray array
        let bitmapCl = context.CreateClArray bitmap

        let actual: ClArray<'a> option =
            testFun processor HostInterop bitmapCl arrayCl

        let actual =
            actual
            |> Option.map (fun a -> a.ToHostAndFree processor)

        arrayCl.Free processor
        bitmapCl.Free processor

        let expected =
            (bitmap, array)
            ||> Array.zip
            |> Array.filter (fun (bit, _) -> bit <> 1)
            |> Array.unzip
            |> snd

        match actual with
        | Some actual ->
            "Results must be the same"
            |> Utils.compareArrays isEqual actual expected
        | None ->
            "Expected should be empty"
            |> Expect.isEmpty expected

let createTest<'a> (zero: 'a) isEqual =
    ClArray.excludeElements context Constants.Common.defaultWorkGroupSize
    |> makeTest<'a> isEqual zero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> 0 (=)

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> 0.0 (=)

      createTest<float32> 0.0f (=)
      createTest<bool> false (=) ]
    |> testList "ExcludeElements tests"
