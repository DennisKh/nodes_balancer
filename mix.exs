defmodule NodesBalancer.MixProject do
  use Mix.Project

  @source_url "https://github.com/DennisKh/nodes_balancer"
  @version "0.0.2"

  def project do
    [
      app: :nodes_balancer,
      version: @version,
      elixir: "~> 1.8",
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env)
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README.md", "LICENSE.md"],
      maintainers: ["Denys Kharchuk"],
      licenses: ["MIT"],
      links: %{GitHub: @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatter_opts: [gfm: true],
      extras: [
        "README.md"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
