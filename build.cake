#tool "nuget:?package=xunit.runner.console&version=2.3.1"
//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument ("target", "Default");
var configuration = Argument ("configuration", "Release");
var version = Argument ("build_version", "1.0.0.0");

Information("target: {0}", target);
Information("configuration: {0}", configuration);
Information("build_version: {0}", version);

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////

// Define directories.
var rootDir = Directory ("./");
var sourceDir = Directory ("./src");
var buildDir = Directory ("./localBuild");
var solutionOutputDir = Directory (buildDir.Path + "/MergeQueryObjectSolution");
var integrationTestOutputDir = Directory (buildDir.Path + "/IntegrationTests");
var mergeQueryObjectOutputDir = Directory (buildDir.Path + "/MergeQueryObjectUtility");

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task ("Clean")
    .Does (() => {
        CleanDirectory (buildDir);
    });

Task ("Restore-NuGet-Packages")
    .IsDependentOn ("Clean")
    .Does (() => {
        NuGetRestore ("./src/MergeQueryObject.sln");
    });

Task ("BuildSolution")
    .IsDependentOn ("Restore-NuGet-Packages")
    .Does (() => {
        var settings = new MSBuildSettings ()
            .SetConfiguration (configuration)
            .SetVerbosity (Verbosity.Minimal)
            .WithProperty ("OutDir", MakeAbsolute (solutionOutputDir).FullPath)
            .WithProperty ("Version", version)
            .WithProperty ("AssemblyVersion", version)
            .WithProperty ("FileVersion", version);

        MSBuild (sourceDir.Path + "/MergeQueryObject.sln", settings);
    });

Task ("BuildIntegrationTests")
    .IsDependentOn ("Restore-NuGet-Packages")
    .Does (() => {
        var settings = new MSBuildSettings ()
            .SetConfiguration (configuration)
            .SetVerbosity (Verbosity.Minimal)
            .WithProperty ("OutDir", MakeAbsolute (integrationTestOutputDir).FullPath)
            .WithProperty ("Version", version)
            .WithProperty ("AssemblyVersion", version)
            .WithProperty ("FileVersion", version);

        MSBuild (sourceDir.Path + "/IntegrationTests/IntegrationTests.csproj", settings);
    });

Task ("BuildMergeQueryObject")
    .IsDependentOn ("Restore-NuGet-Packages")
    .Does (() => {
        var settings = new MSBuildSettings ()
            .SetConfiguration (configuration)
            .SetVerbosity (Verbosity.Minimal)
            .WithProperty ("OutDir", MakeAbsolute (mergeQueryObjectOutputDir).FullPath)
            .WithProperty ("Version", version)
            .WithProperty ("AssemblyVersion", version)
            .WithProperty ("FileVersion", version);

        MSBuild (sourceDir.Path + "/ivaldez.SqlMergeQueryObject/ivaldez.Sql.SqlMergeQueryObject.csproj", settings);
    });

Task ("Run-Unit-Tests")
    .IsDependentOn ("BuildIntegrationTests")
    .Does (() => {
        Information ("Start Running Tests");
        XUnit2 (integrationTestOutputDir.Path + "/*Tests.dll");
    });

Task ("BuildPackages")
    .IsDependentOn ("Restore-NuGet-Packages")
    .IsDependentOn ("BuildMergeQueryObject")
    .Does (() => {
        var settings = new DotNetCorePackSettings {
            Configuration = "Release",
            OutputDirectory = buildDir.Path,
            IncludeSource = true,
            IncludeSymbols = true
        };
        var projectPath = sourceDir.Path + "/ivaldez.SqlMergeQueryObject/ivaldez.Sql.SqlMergeQueryObject.csproj";

        XmlPoke(projectPath, "/Project/PropertyGroup/Version", version);
        XmlPoke(projectPath, "/Project/PropertyGroup/AssemblyVersion", version);

        DotNetCorePack (projectPath, settings);
    });

//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task ("Default")
    .IsDependentOn ("BuildSolution")
    .IsDependentOn ("BuildMergeQueryObject")  
    .IsDependentOn ("BuildIntegrationTests")    
    .IsDependentOn ("Run-Unit-Tests")
    .IsDependentOn ("BuildPackages");

//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget (target);