module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.ChunkBySize

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.Sub> ] }

let makeTestGetChunk<'a when 'a: equality> testFun (array: 'a [], startPosition, count) =

    if array.Length > 0 then

        let clArray = context.CreateClArray array

        let (clActual: ClArray<'a>) =
            testFun processor HostInterop clArray startPosition count

        clArray.Free processor
        let actual = clActual.ToHostAndFree processor

        "Results must be the same"
        |> Expect.sequenceEqual actual (Array.sub array startPosition count)

let creatTestSub<'a when 'a: equality> =
    ClArray.sub context Utils.defaultWorkGroupSize
    |> makeTestGetChunk<'a>
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let subTests =
    [ creatTestSub<int>

      if Utils.isFloat64Available context.ClDevice then
          creatTestSub<float>

      creatTestSub<float32>
      creatTestSub<bool>
      creatTestSub<byte> ]
    |> testList "getChunk"

let makeTestChunkBySize<'a when 'a: equality> isEqual testFun (array: 'a [], chunkSize: int) =

    if chunkSize > 0 && array.Length > 0 then

        let clArray = context.CreateClArray array

        let clActual: ClArray<'a> [] =
            (testFun processor HostInterop chunkSize clArray)

        clArray.Free processor

        let actual =
            clActual
            |> Array.map (fun clArray -> clArray.ToHostAndFree processor)

        let expected = Array.chunkBySize chunkSize array

        "Results must be the same"
        |> Utils.compareChunksArrays isEqual actual expected

let chunkBySizeConfig =
    { config with
          arbitrary = [ typeof<Generators.ChunkBySize> ] }

let creatTestChunkBySize<'a when 'a: equality> isEqual =
    ClArray.chunkBySize context Utils.defaultWorkGroupSize
    |> makeTestChunkBySize<'a> isEqual
    |> testPropertyWithConfig chunkBySizeConfig $"test on %A{typeof<'a>}"

let chunkBySizeTests =
    [ creatTestChunkBySize<int> (=)

      if Utils.isFloat64Available context.ClDevice then
          creatTestChunkBySize<float> Utils.floatIsEqual

      creatTestChunkBySize<float32> Utils.float32IsEqual
      creatTestChunkBySize<bool> (=)
      creatTestChunkBySize<byte> (=) ]
    |> testList "chanBySize"

let creatTestChunkBySizeLazy<'a when 'a: equality> isEqual =
    (fun processor allocationMode chunkSize array ->
        ClArray.lazyChunkBySize context Utils.defaultWorkGroupSize processor allocationMode chunkSize array
        |> Seq.map (fun lazyValue -> lazyValue.Value)
        |> Seq.toArray)
    |> makeTestChunkBySize<'a> isEqual
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let lazyChunkBySizeTests =
    [ creatTestChunkBySizeLazy<int> (=)

      if Utils.isFloat64Available context.ClDevice then
          creatTestChunkBySizeLazy<float> Utils.floatIsEqual

      creatTestChunkBySizeLazy<float32> Utils.float32IsEqual
      creatTestChunkBySizeLazy<bool> (=)
      creatTestChunkBySizeLazy<byte> (=) ]
    |> testList "chunkBySize lazy"

let allTests =
    testList
        "chunk"
        [ subTests
          chunkBySizeTests
          lazyChunkBySizeTests ]
