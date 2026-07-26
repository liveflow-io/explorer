defmodule Explorer.EnumDomain do
  @moduledoc false

  alias Explorer.PolarsBackend.Native

  @enforce_keys [:resource, :size, :fingerprint]
  defstruct [:resource, :size, :fingerprint]

  @type t :: %__MODULE__{
          resource: reference(),
          size: non_neg_integer(),
          fingerprint: non_neg_integer()
        }

  def new(categories), do: Native.enum_domain_new(categories)
  def categories(%__MODULE__{} = domain), do: Native.enum_domain_categories(domain)

  def equal?(
        %__MODULE__{size: size, fingerprint: fingerprint} = left,
        %__MODULE__{size: size, fingerprint: fingerprint} = right
      ),
      do: Native.enum_domain_equal(left, right)

  def equal?(%__MODULE__{}, %__MODULE__{}), do: false

  @doc """
  Checks whether `value` is one of the domain's categories.

  This is a hash lookup on the native domain, so it does not materialize the
  category list.
  """
  def member?(%__MODULE__{} = domain, value) when is_binary(value),
    do: Native.enum_domain_member(domain, value)

  def member?(%__MODULE__{}, _value), do: false
end

# A domain stands in for its category list, so that code reaching into
# `series.dtype` can treat it like the list returned by `Explorer.Series.dtype/1`.
defimpl Enumerable, for: Explorer.EnumDomain do
  alias Explorer.EnumDomain

  def count(domain), do: {:ok, domain.size}
  def member?(domain, value), do: {:ok, EnumDomain.member?(domain, value)}
  def slice(_domain), do: {:error, __MODULE__}

  def reduce(domain, acc, fun), do: Enumerable.reduce(EnumDomain.categories(domain), acc, fun)
end

defimpl Inspect, for: Explorer.EnumDomain do
  import Inspect.Algebra

  def inspect(domain, _opts) do
    concat([
      "#Explorer.EnumDomain<",
      Integer.to_string(domain.size),
      " categories, ",
      Integer.to_string(domain.fingerprint, 16),
      ">"
    ])
  end
end
